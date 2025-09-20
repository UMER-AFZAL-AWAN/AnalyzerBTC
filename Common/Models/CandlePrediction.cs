using Microsoft.ML.Data;

namespace Common.Models
{
    public class CandlePrediction
    {
        [ColumnName("PredictedLabel")]
        public bool PredictedLabel { get; set; }

        public float Probability { get; set; }
        public float Score { get; set; }
    }

}
