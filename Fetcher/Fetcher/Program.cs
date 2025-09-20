using Fetcher.Models;
using Fetcher.Repositories;
using Fetcher.Services;

namespace Fetcher
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            Console.Write("Enter database password: ");
            string password = ReadPassword();

            string connString = $"Host=localhost;Port=5432;Database=BTCUSDT;Username=postgres;Password={password};";
            var repo = new BinanceKlineRepository(connString);
            var service = new BinanceApiService();

            string symbol = "BTCUSDT";
            string interval = "1h"; // ✅ 1-hour candles
            //string interval = "5m"; // ✅ 5 min

            // Where should we start?
            DateTime startTime = (await repo.GetLastCloseTimeAsync(symbol))
                ?? new DateTime(2017, 8, 17, 0, 0, 0, DateTimeKind.Utc);

            Console.WriteLine($"▶ Starting from: {startTime}");

            // Fill history until now
            while (startTime < DateTime.UtcNow)
            {
                var klines = await service.GetKlinesAsync(symbol, interval, startTime, 1000);
                if (klines.Count == 0) break;

                await repo.BulkInsertAsync(klines);
                startTime = klines.Last().CloseTime.AddHours(1); // ✅ step by 1 hour
                //startTime = klines.Last().CloseTime.AddMinutes(5);
                Console.WriteLine($"Inserted up to {startTime}");
                await Task.Delay(50); // ⚡ tiny pause (avoid 429 rate-limit)
            }

            Console.WriteLine("✅ History sync complete. Switching to live mode...");

            // Live mode
            while (true)
            {
                var lastClose = await repo.GetLastCloseTimeAsync(symbol) ?? DateTime.UtcNow;
                var nextStart = lastClose.AddHours(1);
                //var nextStart = lastClose.AddMinutes(5);

                if (nextStart <= DateTime.UtcNow)
                {
                    var klines = await service.GetKlinesAsync(symbol, interval, nextStart, 1);
                    if (klines.Count > 0)
                    {
                        await repo.BulkInsertAsync(klines);
                        Console.WriteLine($"[LIVE] Inserted candle {klines[0].OpenTime}");
                    }
                }

                // allow exit on key press "5"
                if (Console.KeyAvailable && Console.ReadKey(true).KeyChar == '5')
                {
                    Console.WriteLine("⏹ Exiting...");
                    break;
                }

                await Task.Delay(1000); // check every second
            }
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
