using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Fetcher.Utilities
{
    public static class SafeParser
    {
        public static decimal ParseDecimal(string? input)
        {
            if (decimal.TryParse(input, out var value))
                return value;
            return 0m;
        }

        public static int ParseInt(string? input)
        {
            if (int.TryParse(input, out var value))
                return value;
            return 0;
        }
    }
}
