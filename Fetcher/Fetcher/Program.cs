using Fetcher.Models;
using Fetcher.Repositories;
using Fetcher.Utilities;
using System.Text.Json;

namespace Fetcher
{
    internal class Program
    {
        static void Main(string[] args)
        {
            // Secure DB password input
            Console.Write("Enter database password: ");
            string password = ReadPassword();

            string connString = $"Host=localhost;Port=5432;Database=BTCUSDT;Username=postgres;Password={password};";

            var httpClient = new HttpClient { BaseAddress = new Uri("https://api.binance.com") };
            var repo = new BinanceKlineRepository(connString);

            Console.WriteLine("🚀 Starting Binance Kline fetcher...");
            Console.WriteLine("Press '5' anytime to exit.\n");

            while (true)
            {
                if (Console.KeyAvailable && Console.ReadKey(true).Key == ConsoleKey.D5)
                {
                    Console.WriteLine("🛑 Exiting loop...");
                    break;
                }

                try
                {
                    // Fetch last 100 candles
                    var response = httpClient.GetAsync("/api/v3/klines?symbol=BTCUSDT&interval=1m&limit=100")
                                             .GetAwaiter().GetResult();
                    response.EnsureSuccessStatusCode();
                    var json = response.Content.ReadAsStringAsync().GetAwaiter().GetResult();

                    var raw = JsonSerializer.Deserialize<List<List<JsonElement>>>(json);

                    var klines = new List<Kline>();
                    foreach (var k in raw)
                    {
                        klines.Add(new Kline
                        {
                            Symbol = "BTCUSDT",
                            OpenTime = DateTimeOffset.FromUnixTimeMilliseconds(k[0].GetInt64()).UtcDateTime,
                            Open = SafeParser.ParseDecimal(k[1].GetString()),
                            High = SafeParser.ParseDecimal(k[2].GetString()),
                            Low = SafeParser.ParseDecimal(k[3].GetString()),
                            Close = SafeParser.ParseDecimal(k[4].GetString()),
                            Volume = SafeParser.ParseDecimal(k[5].GetString()),
                            CloseTime = DateTimeOffset.FromUnixTimeMilliseconds(k[6].GetInt64()).UtcDateTime,
                            QuoteVolume = SafeParser.ParseDecimal(k[7].GetString()),
                            TradeCount = SafeParser.ParseInt(k[8].GetString()),
                            TakerBuyBaseVolume = SafeParser.ParseDecimal(k[9].GetString()),
                            TakerBuyQuoteVolume = SafeParser.ParseDecimal(k[10].GetString())
                        });
                    }

                    // Store in DB
                    foreach (var k in klines)
                    {
                        var id = repo.InsertKlineAsync(k).GetAwaiter().GetResult();
                        Console.WriteLine($"✅ Inserted Kline at {k.OpenTime} (ID: {id})");
                    }

                    Thread.Sleep(2000); // Avoid API ban
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"❌ Error: {ex.Message}");
                }
            }
        }

        /// <summary>
        /// Secure password input with * masking
        /// </summary>
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
