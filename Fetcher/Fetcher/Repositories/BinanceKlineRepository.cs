using Fetcher.Models;
using Npgsql;

namespace Fetcher.Repositories
{
    internal class BinanceKlineRepository
    {
        private readonly string _connectionString;
        public BinanceKlineRepository(string connectionString)
        {
            _connectionString = connectionString;
        }

        // ---------- CREATE ----------

        public async Task<int> InsertKlineAsync(Kline kline)
        {
            const string sql = @"
    INSERT INTO public.binancekline
    (symbol, opentime, open, high, low, close, volume, closetime, quotevolume, tradecount, takerbuybasevolume, takerbuyquotevolume)
    VALUES (@symbol, @opentime, @open, @high, @low, @close, @volume, @closetime, @quotevolume, @tradecount, @takerbuybasevolume, @takerbuyquotevolume)
    RETURNING id;";

            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync();

            await using var cmd = new NpgsqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("symbol", kline.Symbol ?? (object)DBNull.Value);
            cmd.Parameters.AddWithValue("opentime", kline.OpenTime);
            cmd.Parameters.AddWithValue("open", kline.Open);
            cmd.Parameters.AddWithValue("high", kline.High);
            cmd.Parameters.AddWithValue("low", kline.Low);
            cmd.Parameters.AddWithValue("close", kline.Close);
            cmd.Parameters.AddWithValue("volume", kline.Volume);
            cmd.Parameters.AddWithValue("closetime", kline.CloseTime);
            cmd.Parameters.AddWithValue("quotevolume", kline.QuoteVolume);
            cmd.Parameters.AddWithValue("tradecount", kline.TradeCount);
            cmd.Parameters.AddWithValue("takerbuybasevolume", kline.TakerBuyBaseVolume);
            cmd.Parameters.AddWithValue("takerbuyquotevolume", kline.TakerBuyQuoteVolume);

            var result = await cmd.ExecuteScalarAsync();

            if (result == null || result == DBNull.Value)
                return -1; // or throw an exception

            return Convert.ToInt32(result);
        }


        public async Task BulkInsertAsync(IEnumerable<Kline> klines)
        {
            const string sql = @"
            INSERT INTO public.binancekline
            (symbol, opentime, open, high, low, close, volume, closetime, quotevolume, tradecount, takerbuybasevolume, takerbuyquotevolume)
            VALUES (@symbol, @opentime, @open, @high, @low, @close, @volume, @closetime, @quotevolume, @tradecount, @takerbuybasevolume, @takerbuyquotevolume)
            ON CONFLICT (symbol, opentime) DO NOTHING;";

            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync();
            await using var tx = await conn.BeginTransactionAsync();

            foreach (var k in klines)
            {
                await using var cmd = new NpgsqlCommand(sql, conn, (NpgsqlTransaction)tx);
                cmd.Parameters.AddWithValue("symbol", k.Symbol);
                cmd.Parameters.AddWithValue("opentime", k.OpenTime);
                cmd.Parameters.AddWithValue("open", k.Open);
                cmd.Parameters.AddWithValue("high", k.High);
                cmd.Parameters.AddWithValue("low", k.Low);
                cmd.Parameters.AddWithValue("close", k.Close);
                cmd.Parameters.AddWithValue("volume", k.Volume);
                cmd.Parameters.AddWithValue("closetime", k.CloseTime);
                cmd.Parameters.AddWithValue("quotevolume", k.QuoteVolume);
                cmd.Parameters.AddWithValue("tradecount", k.TradeCount);
                cmd.Parameters.AddWithValue("takerbuybasevolume", k.TakerBuyBaseVolume);
                cmd.Parameters.AddWithValue("takerbuyquotevolume", k.TakerBuyQuoteVolume);

                await cmd.ExecuteNonQueryAsync();
            }

            await tx.CommitAsync();
        }

        // ---------- READ ----------

        public async Task<Kline?> GetByIdAsync(int id)
        {
            const string sql = "SELECT * FROM public.binancekline WHERE id = @id";
            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync();

            await using var cmd = new NpgsqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("id", id);

            await using var reader = await cmd.ExecuteReaderAsync();
            if (await reader.ReadAsync())
            {
                return MapReaderToKline(reader);
            }

            return null;
        }

        public async Task<List<Kline>> GetAllAsync()
        {
            const string sql = "SELECT * FROM public.binancekline ORDER BY opentime ASC";
            var list = new List<Kline>();

            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync();
            await using var cmd = new NpgsqlCommand(sql, conn);
            await using var reader = await cmd.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                list.Add(MapReaderToKline(reader));
            }

            return list;
        }

        public async Task<List<Kline>> GetBySymbolAsync(string symbol)
        {
            const string sql = "SELECT * FROM public.binancekline WHERE symbol = @symbol ORDER BY opentime ASC";
            var list = new List<Kline>();

            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync();
            await using var cmd = new NpgsqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("symbol", symbol);
            await using var reader = await cmd.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                list.Add(MapReaderToKline(reader));
            }

            return list;
        }

        public async Task<DateTime?> GetLastCloseTimeAsync(string symbol)
        {
            const string sql = @"SELECT MAX(closetime) FROM public.binancekline WHERE symbol = @symbol";
            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync();

            await using var cmd = new NpgsqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("symbol", symbol);

            var result = await cmd.ExecuteScalarAsync();
            return result == DBNull.Value ? null : (DateTime?)result;
        }

        // ---------- UPDATE ----------

        public async Task<bool> UpdateKlineAsync(Kline kline)
        {
            const string sql = @"
            UPDATE public.binancekline
            SET open=@open, high=@high, low=@low, close=@close, volume=@volume,
                closetime=@closetime, quotevolume=@quotevolume, tradecount=@tradecount,
                takerbuybasevolume=@takerbuybasevolume, takerbuyquotevolume=@takerbuyquotevolume
            WHERE id=@id";

            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync();
            await using var cmd = new NpgsqlCommand(sql, conn);

            cmd.Parameters.AddWithValue("id", kline.Id);
            cmd.Parameters.AddWithValue("open", kline.Open);
            cmd.Parameters.AddWithValue("high", kline.High);
            cmd.Parameters.AddWithValue("low", kline.Low);
            cmd.Parameters.AddWithValue("close", kline.Close);
            cmd.Parameters.AddWithValue("volume", kline.Volume);
            cmd.Parameters.AddWithValue("closetime", kline.CloseTime);
            cmd.Parameters.AddWithValue("quotevolume", kline.QuoteVolume);
            cmd.Parameters.AddWithValue("tradecount", kline.TradeCount);
            cmd.Parameters.AddWithValue("takerbuybasevolume", kline.TakerBuyBaseVolume);
            cmd.Parameters.AddWithValue("takerbuyquotevolume", kline.TakerBuyQuoteVolume);

            var rows = await cmd.ExecuteNonQueryAsync();
            return rows > 0;
        }

        // ---------- DELETE ----------

        public async Task<bool> DeleteByIdAsync(int id)
        {
            const string sql = "DELETE FROM public.binancekline WHERE id=@id";
            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync();

            await using var cmd = new NpgsqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("id", id);

            var rows = await cmd.ExecuteNonQueryAsync();
            return rows > 0;
        }

        public async Task<int> DeleteBySymbolAsync(string symbol)
        {
            const string sql = "DELETE FROM public.binancekline WHERE symbol=@symbol";
            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync();

            await using var cmd = new NpgsqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("symbol", symbol);

            var rows = await cmd.ExecuteNonQueryAsync();
            return rows;
        }

        // ---------- HELPER ----------

        private static Kline MapReaderToKline(NpgsqlDataReader reader)
        {
            return new Kline
            {
                Id = reader.GetInt32(reader.GetOrdinal("id")),
                Symbol = reader.IsDBNull(reader.GetOrdinal("symbol"))
                            ? string.Empty
                            : reader.GetString(reader.GetOrdinal("symbol")),
                OpenTime = reader.IsDBNull(reader.GetOrdinal("opentime"))
                            ? DateTime.MinValue
                            : reader.GetDateTime(reader.GetOrdinal("opentime")),
                Open = reader.IsDBNull(reader.GetOrdinal("open"))
                            ? 0m
                            : reader.GetDecimal(reader.GetOrdinal("open")),
                High = reader.IsDBNull(reader.GetOrdinal("high"))
                            ? 0m
                            : reader.GetDecimal(reader.GetOrdinal("high")),
                Low = reader.IsDBNull(reader.GetOrdinal("low"))
                            ? 0m
                            : reader.GetDecimal(reader.GetOrdinal("low")),
                Close = reader.IsDBNull(reader.GetOrdinal("close"))
                            ? 0m
                            : reader.GetDecimal(reader.GetOrdinal("close")),
                Volume = reader.IsDBNull(reader.GetOrdinal("volume"))
                            ? 0m
                            : reader.GetDecimal(reader.GetOrdinal("volume")),
                CloseTime = reader.IsDBNull(reader.GetOrdinal("closetime"))
                            ? DateTime.MinValue
                            : reader.GetDateTime(reader.GetOrdinal("closetime")),
                QuoteVolume = reader.IsDBNull(reader.GetOrdinal("quotevolume"))
                            ? 0m
                            : reader.GetDecimal(reader.GetOrdinal("quotevolume")),
                TradeCount = reader.IsDBNull(reader.GetOrdinal("tradecount"))
                            ? 0
                            : reader.GetInt32(reader.GetOrdinal("tradecount")),
                TakerBuyBaseVolume = reader.IsDBNull(reader.GetOrdinal("takerbuybasevolume"))
                            ? 0m
                            : reader.GetDecimal(reader.GetOrdinal("takerbuybasevolume")),
                TakerBuyQuoteVolume = reader.IsDBNull(reader.GetOrdinal("takerbuyquotevolume"))
                            ? 0m
                            : reader.GetDecimal(reader.GetOrdinal("takerbuyquotevolume"))
            };
        }

    }
}
