using Fetcher.Models;
using Fetcher.Repositories;
using Fetcher.Services;
using System.Text.Json;

namespace Fetcher
{
    internal class Program
    {
        static void Main(string[] args)
        {
            // Ask for DB password securely
            Console.Write("Enter database password: ");
            string password = ReadPassword();

            string connString = $"Host=localhost;Port=5432;Database=BTCUSDT;Username=postgres;Password={password};";

            var httpClient = new HttpClient { BaseAddress = new Uri("https://api.binance.com") };
            var repo = new BinanceKlineRepository(connString);

            // 1. Fetch hourly candles
            var response = httpClient.GetAsync("/api/v3/klines?symbol=BTCUSDT&interval=1h&limit=50")
                                     .GetAwaiter().GetResult();

            response.EnsureSuccessStatusCode();
            var json = response.Content.ReadAsStringAsync().GetAwaiter().GetResult();

            // 2. Parse JSON
            var raw = JsonSerializer.Deserialize<List<List<JsonElement>>>(json);

            var klines = new List<Kline>();
            foreach (var k in raw)
            {
                klines.Add(new Kline
                {
                    Symbol = "BTCUSDT",
                    OpenTime = DateTimeOffset.FromUnixTimeMilliseconds(k[0].GetInt64()).UtcDateTime,
                    Open = decimal.Parse(k[1].GetString()),
                    High = decimal.Parse(k[2].GetString()),
                    Low = decimal.Parse(k[3].GetString()),
                    Close = decimal.Parse(k[4].GetString()),
                    Volume = decimal.Parse(k[5].GetString()),
                    CloseTime = DateTimeOffset.FromUnixTimeMilliseconds(k[6].GetInt64()).UtcDateTime,
                    QuoteVolume = decimal.Parse(k[7].GetString()),
                    TradeCount = k[8].GetInt32(),
                    TakerBuyBaseVolume = decimal.Parse(k[9].GetString()),
                    TakerBuyQuoteVolume = decimal.Parse(k[10].GetString())
                });
            }

            // 3. Store into DB
            foreach (var k in klines)
            {
                var newId = repo.InsertKlineAsync(k).GetAwaiter().GetResult();
                Console.WriteLine($"Inserted Kline at {k.OpenTime} with Id {newId}");
            }

            Console.WriteLine("✅ All candles stored in DB.");
        }

        /// <summary>
        /// Reads password from console without echoing characters.
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
                    Console.Write("*"); // Mask with *
                }
            } while (key.Key != ConsoleKey.Enter);

            Console.WriteLine();
            return password;
        }
    }
}
