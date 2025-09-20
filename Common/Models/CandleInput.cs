using Microsoft.ML.Data;

namespace Common.Models
{
    public class CandleInput
    {
        public float Open { get; set; }
        public float High { get; set; }
        public float Low { get; set; }
        public float Close { get; set; }
        public float Volume { get; set; }

        [ColumnName("Label")]
        public bool Label { get; set; }

        // Optional convenience: create from Common.Models.Kline
        public static CandleInput FromKline(Common.Models.Kline k)
            => new CandleInput
            {
                Open = (float)k.Open,
                High = (float)k.High,
                Low = (float)k.Low,
                Close = (float)k.Close,
                Volume = (float)k.Volume,
                Label = false
            };
    }

}
