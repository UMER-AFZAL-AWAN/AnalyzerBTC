using Fetcher.Models;
using Npgsql;
namespace Fetcher.Repositories
{
    internal class BinanceTickerRepository
    {
        private readonly string _connectionString;

        public BinanceTickerRepository(string connectionString)
        {
            _connectionString = connectionString;
        }

        public async Task<int> InsertTickerAsync(BinanceTicker ticker)
        {
            const string sql = @"
            INSERT INTO BinanceTicker 
            (Symbol, PriceChange, PriceChangePercent, WeightedAvgPrice, PrevClosePrice, LastPrice, LastQty,
             BidPrice, AskPrice, OpenPrice, HighPrice, LowPrice, Volume, QuoteVolume, OpenTime, CloseTime, 
             FirstId, LastId, Count)
            VALUES 
            (@Symbol, @PriceChange, @PriceChangePercent, @WeightedAvgPrice, @PrevClosePrice, @LastPrice, @LastQty,
             @BidPrice, @AskPrice, @OpenPrice, @HighPrice, @LowPrice, @Volume, @QuoteVolume, @OpenTime, @CloseTime,
             @FirstId, @LastId, @Count)
            RETURNING Id;";

            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync();

            await using var cmd = new NpgsqlCommand(sql, conn);

            cmd.Parameters.AddWithValue("Symbol", ticker.Symbol);
            cmd.Parameters.AddWithValue("PriceChange", ticker.PriceChange);
            cmd.Parameters.AddWithValue("PriceChangePercent", ticker.PriceChangePercent);
            cmd.Parameters.AddWithValue("WeightedAvgPrice", ticker.WeightedAvgPrice);
            cmd.Parameters.AddWithValue("PrevClosePrice", ticker.PrevClosePrice);
            cmd.Parameters.AddWithValue("LastPrice", ticker.LastPrice);
            cmd.Parameters.AddWithValue("LastQty", ticker.LastQty);
            cmd.Parameters.AddWithValue("BidPrice", ticker.BidPrice);
            cmd.Parameters.AddWithValue("AskPrice", ticker.AskPrice);
            cmd.Parameters.AddWithValue("OpenPrice", ticker.OpenPrice);
            cmd.Parameters.AddWithValue("HighPrice", ticker.HighPrice);
            cmd.Parameters.AddWithValue("LowPrice", ticker.LowPrice);
            cmd.Parameters.AddWithValue("Volume", ticker.Volume);
            cmd.Parameters.AddWithValue("QuoteVolume", ticker.QuoteVolume);
            cmd.Parameters.AddWithValue("OpenTime", ticker.OpenTime);
            cmd.Parameters.AddWithValue("CloseTime", ticker.CloseTime);
            cmd.Parameters.AddWithValue("FirstId", ticker.FirstId);
            cmd.Parameters.AddWithValue("LastId", ticker.LastId);
            cmd.Parameters.AddWithValue("Count", ticker.Count);

            var id = (int)await cmd.ExecuteScalarAsync();
            return id;
        }

        public async Task<BinanceTicker?> GetTickerByIdAsync(int id)
        {
            const string sql = "SELECT * FROM BinanceTicker WHERE Id = @Id";

            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync();

            await using var cmd = new NpgsqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("Id", id);

            await using var reader = await cmd.ExecuteReaderAsync();
            if (await reader.ReadAsync())
            {
                return MapTicker(reader);
            }

            return null;
        }

        public async Task<List<BinanceTicker>> GetAllTickersAsync()
        {
            const string sql = "SELECT * FROM BinanceTicker ORDER BY CreatedAt DESC";

            var list = new List<BinanceTicker>();

            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync();

            await using var cmd = new NpgsqlCommand(sql, conn);
            await using var reader = await cmd.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                list.Add(MapTicker(reader));
            }

            return list;
        }

        public async Task UpdateLastPriceAsync(int id, decimal newPrice)
        {
            const string sql = "UPDATE BinanceTicker SET LastPrice = @LastPrice WHERE Id = @Id";

            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync();

            await using var cmd = new NpgsqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("LastPrice", newPrice);
            cmd.Parameters.AddWithValue("Id", id);

            await cmd.ExecuteNonQueryAsync();
        }

        public async Task DeleteTickerAsync(int id)
        {
            const string sql = "DELETE FROM BinanceTicker WHERE Id = @Id";

            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync();

            await using var cmd = new NpgsqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("Id", id);

            await cmd.ExecuteNonQueryAsync();
        }

        private BinanceTicker MapTicker(NpgsqlDataReader reader)
        {
            return new BinanceTicker
            {
                Symbol = reader["Symbol"].ToString(),
                PriceChange = reader.GetFieldValue<decimal>(reader.GetOrdinal("PriceChange")),
                PriceChangePercent = reader.GetFieldValue<decimal>(reader.GetOrdinal("PriceChangePercent")),
                WeightedAvgPrice = reader.GetFieldValue<decimal>(reader.GetOrdinal("WeightedAvgPrice")),
                PrevClosePrice = reader.GetFieldValue<decimal>(reader.GetOrdinal("PrevClosePrice")),
                LastPrice = reader.GetFieldValue<decimal>(reader.GetOrdinal("LastPrice")),
                LastQty = reader.GetFieldValue<decimal>(reader.GetOrdinal("LastQty")),
                BidPrice = reader.GetFieldValue<decimal>(reader.GetOrdinal("BidPrice")),
                AskPrice = reader.GetFieldValue<decimal>(reader.GetOrdinal("AskPrice")),
                OpenPrice = reader.GetFieldValue<decimal>(reader.GetOrdinal("OpenPrice")),
                HighPrice = reader.GetFieldValue<decimal>(reader.GetOrdinal("HighPrice")),
                LowPrice = reader.GetFieldValue<decimal>(reader.GetOrdinal("LowPrice")),
                Volume = reader.GetFieldValue<decimal>(reader.GetOrdinal("Volume")),
                QuoteVolume = reader.GetFieldValue<decimal>(reader.GetOrdinal("QuoteVolume")),
                OpenTime = reader.GetFieldValue<long>(reader.GetOrdinal("OpenTime")),
                CloseTime = reader.GetFieldValue<long>(reader.GetOrdinal("CloseTime")),
                FirstId = reader.GetFieldValue<long>(reader.GetOrdinal("FirstId")),
                LastId = reader.GetFieldValue<long>(reader.GetOrdinal("LastId")),
                Count = reader.GetFieldValue<long>(reader.GetOrdinal("Count"))
            };
        }

    }
}
