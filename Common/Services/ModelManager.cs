using Common.Models;
using Microsoft.ML;
using Microsoft.ML.Data;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Common.Services
{
    public class ModelManager
    {
        private readonly MLContext _ml;
        private readonly string _trainedDir;
        private readonly string _supervisedPath;
        private readonly string _clusteringPath;

        public ITransformer? SupervisedModel { get; private set; }
        public DataViewSchema? SupervisedSchema { get; private set; }

        public ITransformer? ClusteringModel { get; private set; }
        public DataViewSchema? ClusteringSchema { get; private set; }

        public ModelManager(MLContext ml)
        {
            _ml = ml;

            var solutionRoot = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, @"..\..\..\..\"));
            _trainedDir = Path.Combine(solutionRoot, "Trained");

            if (!Directory.Exists(_trainedDir))
                Directory.CreateDirectory(_trainedDir);

            _supervisedPath = Path.Combine(_trainedDir, "supervisedModel.zip");
            _clusteringPath = Path.Combine(_trainedDir, "clusteringModel.zip");
        }


        // --- Model existence checks ---
        public bool SupervisedModelExists() => File.Exists(_supervisedPath);
        public bool ClusteringModelExists() => File.Exists(_clusteringPath);

        // --- Save ---
        public void SaveSupervisedModel(ITransformer model, DataViewSchema schema)
        {
            _ml.Model.Save(model, schema, _supervisedPath);
            SupervisedModel = model;
            SupervisedSchema = schema;
        }

        public void SaveClusteringModel(ITransformer model, DataViewSchema schema)
        {
            _ml.Model.Save(model, schema, _clusteringPath);
            ClusteringModel = model;
            ClusteringSchema = schema;
        }

        // --- Load ---
        public void LoadModelsIfExists()
        {
            if (SupervisedModelExists())
            {
                SupervisedModel = _ml.Model.Load(_supervisedPath, out var schema);
                SupervisedSchema = schema;
                Console.WriteLine($"Loaded supervised model from {_supervisedPath}");
            }

            if (ClusteringModelExists())
            {
                ClusteringModel = _ml.Model.Load(_clusteringPath, out var schema);
                ClusteringSchema = schema;
                Console.WriteLine($"Loaded clustering model from {_clusteringPath}");
            }
        }

        // --- Prediction (supervised) ---
        public CandlePrediction Predict(CandleInput input)
        {
            if (SupervisedModel == null || SupervisedSchema == null)
                throw new InvalidOperationException("Supervised model not loaded.");

            var predEngine = _ml.Model.CreatePredictionEngine<CandleInput, CandlePrediction>(SupervisedModel, SupervisedSchema);
            return predEngine.Predict(input);
        }

        // --- Training supervised (binary classification) ---
        public (ITransformer model, DataViewSchema schema) TrainSupervised(IEnumerable<CandleInput> labeledData)
        {
            var dataView = _ml.Data.LoadFromEnumerable(labeledData);

            var pipeline = _ml.Transforms.Concatenate("Features",
                    nameof(CandleInput.Open),
                    nameof(CandleInput.High),
                    nameof(CandleInput.Low),
                    nameof(CandleInput.Close),
                    nameof(CandleInput.Volume))
                .Append(_ml.Transforms.NormalizeMinMax("Features"))
                .Append(_ml.BinaryClassification.Trainers.FastTree(
                    labelColumnName: "Label",
                    featureColumnName: "Features"));

            Console.WriteLine($"Training supervised model on {labeledData.Count()} samples...");
            var model = pipeline.Fit(dataView);
            SaveSupervisedModel(model, dataView.Schema);
            Console.WriteLine("Supervised model trained & saved.");
            return (model, dataView.Schema);
        }

        // --- Training clustering (unsupervised KMeans) ---
        public (ITransformer model, DataViewSchema schema) TrainClustering(IEnumerable<CandleInput> unsupervisedData, int clusters = 5)
        {
            var dataView = _ml.Data.LoadFromEnumerable(unsupervisedData);

            var pipeline = _ml.Transforms.Concatenate("Features",
                    nameof(CandleInput.Open),
                    nameof(CandleInput.High),
                    nameof(CandleInput.Low),
                    nameof(CandleInput.Close),
                    nameof(CandleInput.Volume))
                .Append(_ml.Transforms.NormalizeMinMax("Features"))
                .Append(_ml.Clustering.Trainers.KMeans("Features", numberOfClusters: clusters));

            Console.WriteLine($"Training clustering model (KMeans, {clusters} clusters) on {unsupervisedData.Count()} samples...");
            var model = pipeline.Fit(dataView);
            SaveClusteringModel(model, dataView.Schema);
            Console.WriteLine("Clustering model trained & saved.");
            return (model, dataView.Schema);
        }

        // --- Predict cluster ID for one candle ---
        public int GetClusterId(CandleInput input)
        {
            if (ClusteringModel == null || ClusteringSchema == null)
                throw new InvalidOperationException("Clustering model not loaded.");

            var predEngine = _ml.Model.CreatePredictionEngine<CandleInput, ClusterPrediction>(ClusteringModel, ClusteringSchema);
            var pred = predEngine.Predict(input);
            return (int)(pred.PredictedClusterId - 1); // ML.NET cluster ids are 1-based
        }

        // --- Inner class for clustering output ---
        private class ClusterPrediction
        {
            [ColumnName("PredictedLabel")]
            public uint PredictedClusterId { get; set; }
            [ColumnName("Score")]
            public float[]? Distances { get; set; }
        }
    }
}
