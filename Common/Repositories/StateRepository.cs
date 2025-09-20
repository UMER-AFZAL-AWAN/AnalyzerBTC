using Npgsql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Common.Repositories
{
    public class StateRepository
    {
        private readonly string _connString;
        public StateRepository(string connString) => _connString = connString;

        public async Task EnsureStateTableAsync()
        {
            const string sql = @"CREATE TABLE IF NOT EXISTS processor_state (
    id integer PRIMARY KEY,
    last_processed timestamptz NULL,
    updated_at timestamptz NOT NULL DEFAULT NOW()
);
-- ensure one row with id=1 exists
INSERT INTO processor_state (id) VALUES (1)
ON CONFLICT (id) DO NOTHING;
";
            await using var conn = new NpgsqlConnection(_connString);
            await conn.OpenAsync();
            await using var cmd = new NpgsqlCommand(sql, conn);
            await cmd.ExecuteNonQueryAsync();
        }

        public async Task<DateTime?> GetLastProcessedAsync()
        {
            const string sql = "SELECT last_processed FROM processor_state WHERE id = 1";
            await using var conn = new NpgsqlConnection(_connString);
            await conn.OpenAsync();
            await using var cmd = new NpgsqlCommand(sql, conn);
            var res = await cmd.ExecuteScalarAsync();
            if (res == DBNull.Value || res == null) return null;
            return (DateTime)res;
        }

        public async Task SetLastProcessedAsync(DateTime dt)
        {
            const string sql = "UPDATE processor_state SET last_processed=@t, updated_at = NOW() WHERE id = 1";
            await using var conn = new NpgsqlConnection(_connString);
            await conn.OpenAsync();
            await using var cmd = new NpgsqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("t", dt);
            await cmd.ExecuteNonQueryAsync();
        }
    }

}
