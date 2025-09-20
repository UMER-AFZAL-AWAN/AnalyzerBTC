using Common.Models;
using Common.Repositories;
using Common.Services;
using Microsoft.ML;

namespace Processor
{
    internal class Program
    {
             // thresholds
        private const int SupervisedInitialCount = 35000;
        private const int RetrainBufferThreshold = 200;      // retrain after 200 new labeled examples
        private const int ClusteringRetrainInterval = 1000; // retrain clustering after this many new candles (or time-based)

        static async Task Main(string[] args)
        {
            Console.Write("Enter DB password: ");
            string password = ReadPassword();
            string connString = $"Host=localhost;Port=5432;Database=BTCUSDT;Username=postgres;Password={password};";

            var repo = new BinanceKlineRepository(connString);
            var stateRepo = new StateRepository(connString);

            // ensure state table exists
            await stateRepo.EnsureStateTableAsync();

            var mlContext = new MLContext(seed: 42);
            var manager = new ModelManager(mlContext);

            // 1) Load candles from DB (full history)
            Console.WriteLine("Loading candles from DB...");
            var allKlines = await repo.GetBySymbolAsync("BTCUSDT");
            Console.WriteLine($"Loaded {allKlines.Count} klines.");

            if (allKlines.Count < 10)
            {
                Console.WriteLine("Not enough data. Exiting.");
                return;
            }

            // 2) Attempt to load existing models; otherwise train initial models
            manager.LoadModelsIfExists();

            if (manager.SupervisedModel == null)
            {
                Console.WriteLine("No supervised model found. Training initial supervised model (first 35k rows)...");
                var supervisedSource = BuildSupervisedFromKlines(allKlines.Take(Math.Min(SupervisedInitialCount, allKlines.Count)).ToList());
                manager.TrainSupervised(supervisedSource); // saves to disk
            }

            if (manager.ClusteringModel == null)
            {
                Console.WriteLine("No clustering model found. Training clustering on rows after 35k (if available)...");
                var after = allKlines.Skip(SupervisedInitialCount).ToList();
                if (after.Count == 0)
                {
                    // fallback: train clustering on entire set if there's no remainder
                    after = allKlines.ToList();
                }
                var unsup = after.Select(k => CandleInput.FromKline(k)).ToList();
                manager.TrainClustering(unsup, clusters: 5);
            }

            // 3) Determine last processed time (from state table). If none, set to the max closetime present BEFORE training window end.
            var lastProcessed = await stateRepo.GetLastProcessedAsync();
            if (lastProcessed == null)
            {
                // default to last candle's CloseTime at startup (so we won't reprocess whole history)
                // If you want to process all historical rows, set this to DateTime.MinValue
                var defaultStart = allKlines.Skip(Math.Max(0, allKlines.Count - 1)).First().CloseTime;
                lastProcessed = defaultStart;
                await stateRepo.SetLastProcessedAsync(defaultStart);
                Console.WriteLine($"No previous state found. Setting last_processed to {defaultStart}.");
            }
            else
            {
                Console.WriteLine($"Resuming from last_processed = {lastProcessed.Value}.");
            }

            // buffer for new labeled examples (for retraining)
            var retrainBuffer = new List<CandleInput>();
            int processedSinceClusteringRetrain = 0;

            // 4) Continuous loop: check for new candles, predict & learn
            Console.WriteLine("Entering main loop. Press Ctrl+C to exit.");

            while (true)
            {
                try
                {
                    // reload only new candles from DB (we fetch all and filter; you can optimize repository later)
                    var currentKlines = await repo.GetBySymbolAsync("BTCUSDT");
                    var newOnes = currentKlines
                        .Where(k => k.CloseTime > lastProcessed.Value)
                        .OrderBy(k => k.CloseTime)
                        .ToList();

                    if (newOnes.Count == 0)
                    {
                        // no new candles yet
                        await Task.Delay(TimeSpan.FromSeconds(10));
                        continue;
                    }

                    foreach (var candle in newOnes)
                    {
                        // to predict candle X, we use previous candle (previous in DB chronology)
                        var prev = currentKlines.Where(k => k.CloseTime < candle.CloseTime).OrderByDescending(k => k.CloseTime).FirstOrDefault();
                        if (prev == null)
                        {
                            // can't predict without previous candle
                            lastProcessed = candle.CloseTime;
                            await stateRepo.SetLastProcessedAsync(lastProcessed.Value);
                            continue;
                        }

                        var input = CandleInput.FromKline(prev);
                        // supervised prediction
                        var pred = manager.Predict(input);
                        var predicted = pred.PredictedLabel;
                        var prob = pred.Probability;

                        var actual = candle.Close > candle.Open;

                        Console.WriteLine($"{candle.CloseTime:yyyy-MM-dd HH:mm} Predicted: {(predicted ? "GREEN" : "RED")} (p={prob:P2}), Actual: {(actual ? "GREEN" : "RED")}");

                        // If prediction wrong, add labeled sample (prev features labeled with actual next)
                        if (predicted != actual)
                        {
                            var labeled = CandleInput.FromKline(prev);
                            labeled.Label = actual;
                            retrainBuffer.Add(labeled);
                        }

                        // optionally always add the labeled sample to buffer to increase training set
                        {
                            var labeledAll = CandleInput.FromKline(prev);
                            labeledAll.Label = actual;
                            // we can add to a larger history if desired; here we add only to retrainBuffer to retrain faster
                            // Do not add duplicates if you re-run; this is a simple demo.
                        }

                        lastProcessed = candle.CloseTime;
                        await stateRepo.SetLastProcessedAsync(lastProcessed.Value);

                        processedSinceClusteringRetrain++;

                        // If buffer reached threshold, retrain supervised model
                        if (retrainBuffer.Count >= RetrainBufferThreshold)
                        {
                            Console.WriteLine($"Retrain threshold reached ({retrainBuffer.Count}). Rebuilding supervised training dataset...");

                            // Build a new supervised training set:
                            // Here we choose to build supervised dataset from:
                            //  - original first 35k (or less if DB smaller)
                            //  - plus all labeled examples available now (from full history)
                            var allKlinesForTraining = currentKlines; // full history available
                            var supervisedTrainSet = BuildSupervisedFromKlines(allKlinesForTraining);
                            // Optionally: shuffle / limit to latest N samples to keep training size manageable
                            manager.TrainSupervised(supervisedTrainSet);

                            retrainBuffer.Clear();
                        }

                        // Periodically retrain clustering (very simple logic)
                        if (processedSinceClusteringRetrain >= ClusteringRetrainInterval)
                        {
                            Console.WriteLine("Retraining clustering model on recent data...");
                            var unsupervisedRecent = currentKlines.Skip(SupervisedInitialCount).Select(k => CandleInput.FromKline(k)).ToList();
                            if (unsupervisedRecent.Count > 0)
                            {
                                manager.TrainClustering(unsupervisedRecent, clusters: 5);
                            }
                            processedSinceClusteringRetrain = 0;
                        }
                    } // foreach new candle

                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error in main loop: " + ex.Message);
                    Console.WriteLine(ex.StackTrace);
                    // continue after delay
                }

                // small delay before checking again
                await Task.Delay(TimeSpan.FromSeconds(5));
            } // while
        } // Main

        static List<CandleInput> BuildSupervisedFromKlines(List<Common.Models.Kline> klines)
        {
            var list = new List<CandleInput>();
            for (int i = 0; i < klines.Count - 1; i++)
            {
                var curr = klines[i];
                var next = klines[i + 1];
                var inpt = CandleInput.FromKline(curr);
                inpt.Label = next.Close > next.Open;
                list.Add(inpt);
            }
            return list;
        }

        static string ReadPassword()
        {
            var password = string.Empty;
            ConsoleKeyInfo key;
            do
            {
                key = Console.ReadKey(intercept: true);
                if (key.Key == ConsoleKey.Backspace && password.Length > 0)
                {
                    password = password[..^1];
                    Console.Write("\b \b");
                }
                else if (!char.IsControl(key.KeyChar))
                {
                    password += key.KeyChar;
                    Console.Write("*");
                }
            } while (key.Key != ConsoleKey.Enter);

            Console.WriteLine();
            return password;
        }
    }
}
