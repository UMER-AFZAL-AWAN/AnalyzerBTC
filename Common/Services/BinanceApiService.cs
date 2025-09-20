using Common.Models;
using System.Text.Json;

namespace Common.Services
{
    public class BinanceApiService
    {
        private readonly HttpClient _httpClient;

        public BinanceApiService()
        {
            _httpClient = new HttpClient { BaseAddress = new Uri("https://api.binance.com") };
        }

        public async Task<List<Kline>> GetKlinesAsync(string symbol, string interval, DateTime startTime, int limit = 1000)
        {
            long startMs = new DateTimeOffset(startTime).ToUnixTimeMilliseconds();
            string url = $"/api/v3/klines?symbol={symbol}&interval={interval}&startTime={startMs}&limit={limit}";

            var response = await _httpClient.GetAsync(url);
            response.EnsureSuccessStatusCode();
            var json = await response.Content.ReadAsStringAsync();

            var raw = JsonSerializer.Deserialize<List<List<JsonElement>>>(json);
            var klines = new List<Kline>();

            foreach (var k in raw!)
            {
                klines.Add(new Kline
                {
                    Symbol = symbol,
                    OpenTime = DateTimeOffset.FromUnixTimeMilliseconds(k[0].GetInt64()).UtcDateTime,
                    Open = SafeParseDecimal(k[1].GetString()),
                    High = SafeParseDecimal(k[2].GetString()),
                    Low = SafeParseDecimal(k[3].GetString()),
                    Close = SafeParseDecimal(k[4].GetString()),
                    Volume = SafeParseDecimal(k[5].GetString()),
                    CloseTime = DateTimeOffset.FromUnixTimeMilliseconds(k[6].GetInt64()).UtcDateTime,
                    QuoteVolume = SafeParseDecimal(k[7].GetString()),
                    TradeCount = k[8].GetInt32(),
                    TakerBuyBaseVolume = SafeParseDecimal(k[9].GetString()),
                    TakerBuyQuoteVolume = SafeParseDecimal(k[10].GetString())
                });
            }
            return klines;
        }

        private static decimal SafeParseDecimal(string? s)
            => decimal.TryParse(s, out var val) ? val : 0m;
    }
}