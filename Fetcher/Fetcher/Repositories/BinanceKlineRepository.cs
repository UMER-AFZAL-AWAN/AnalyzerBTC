using Fetcher.Models;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Fetcher.Repositories
{
    internal class BinanceKlineRepository
    {
        private readonly string _connectionString;

        public BinanceKlineRepository(string connectionString)
        {
            _connectionString = connectionString;
        }

        public async Task<int> InsertKlineAsync(Kline kline)
        {
            const string sql = @"
            INSERT INTO BinanceKline
            (Symbol, OpenTime, Open, High, Low, Close, Volume, CloseTime, QuoteVolume, 
             TradeCount, TakerBuyBaseVolume, TakerBuyQuoteVolume)
            VALUES
            (@Symbol, @OpenTime, @Open, @High, @Low, @Close, @Volume, @CloseTime, @QuoteVolume,
             @TradeCount, @TakerBuyBaseVolume, @TakerBuyQuoteVolume)
            RETURNING Id;";

            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync();

            await using var cmd = new NpgsqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("OpenTime", kline.OpenTime);
            cmd.Parameters.AddWithValue("CloseTime", kline.CloseTime);
            cmd.Parameters.AddWithValue("Symbol", kline.Symbol);
            cmd.Parameters.AddWithValue("Open", kline.Open);
            cmd.Parameters.AddWithValue("High", kline.High);
            cmd.Parameters.AddWithValue("Low", kline.Low);
            cmd.Parameters.AddWithValue("Close", kline.Close);
            cmd.Parameters.AddWithValue("Volume", kline.Volume);
            cmd.Parameters.AddWithValue("QuoteVolume", kline.QuoteVolume);
            cmd.Parameters.AddWithValue("TradeCount", kline.TradeCount);
            cmd.Parameters.AddWithValue("TakerBuyBaseVolume", kline.TakerBuyBaseVolume);
            cmd.Parameters.AddWithValue("TakerBuyQuoteVolume", kline.TakerBuyQuoteVolume);

            var id = (int)await cmd.ExecuteScalarAsync();
            return id;
        }

        public async Task<List<Kline>> GetAllKlinesAsync()
        {
            const string sql = "SELECT * FROM BinanceKline ORDER BY OpenTime DESC";

            var list = new List<Kline>();

            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync();

            await using var cmd = new NpgsqlCommand(sql, conn);
            await using var reader = await cmd.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                list.Add(new Kline
                {
                    Symbol = reader["Symbol"].ToString(),
                    OpenTime = reader.GetDateTime(reader.GetOrdinal("OpenTime")),
                    Open = reader.GetFieldValue<decimal>(reader.GetOrdinal("Open")),
                    High = reader.GetFieldValue<decimal>(reader.GetOrdinal("High")),
                    Low = reader.GetFieldValue<decimal>(reader.GetOrdinal("Low")),
                    Close = reader.GetFieldValue<decimal>(reader.GetOrdinal("Close")),
                    Volume = reader.GetFieldValue<decimal>(reader.GetOrdinal("Volume")),
                    CloseTime = reader.GetDateTime(reader.GetOrdinal("CloseTime")),
                    QuoteVolume = reader.GetFieldValue<decimal>(reader.GetOrdinal("QuoteVolume")),
                    TradeCount = reader.GetInt32(reader.GetOrdinal("TradeCount")),
                    TakerBuyBaseVolume = reader.GetFieldValue<decimal>(reader.GetOrdinal("TakerBuyBaseVolume")),
                    TakerBuyQuoteVolume = reader.GetFieldValue<decimal>(reader.GetOrdinal("TakerBuyQuoteVolume"))
                });
            }

            return list;
        }

    }
}
