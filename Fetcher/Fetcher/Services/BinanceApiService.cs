using Fetcher.Models;
using System.Text.Json;

namespace Fetcher.Services
{
    internal class BinanceApiService
    {
        private static readonly HttpClient _httpClient = new HttpClient
        {
            BaseAddress = new Uri("https://api.binance.com")
        };

        public async Task<BinanceTicker> GetBtcUsdtTickerAsync()
        {
            var response = await _httpClient.GetAsync("/api/v3/ticker/24hr?symbol=BTCUSDT");
            response.EnsureSuccessStatusCode();

            var json = await response.Content.ReadAsStringAsync();
            var ticker = JsonSerializer.Deserialize<BinanceTicker>(json);

            return ticker;
        }

    }
}
