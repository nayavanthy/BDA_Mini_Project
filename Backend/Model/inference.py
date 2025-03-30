from pyspark.sql import SparkSession
from pyspark.ml.evaluation import RegressionEvaluator

# Initialize Spark session
spark = SparkSession.builder.appName("StockEvaluation").getOrCreate()

# HDFS paths
hdfs_path = "hdfs://localhost:9000/user/bda_mini_project/inferred_data/"
forecast_lstm_path = "hdfs://localhost:9000/user/bda_mini_project/forecast_lstm/"
forecast_arima_path = "hdfs://localhost:9000/user/bda_mini_project/forecast_arima/"
forecast_normal_path = "hdfs://localhost:9000/user/bda_mini_project/forecast/"
final_output_path = "hdfs://localhost:9000/user/bda_mini_project/evaluation_metrics.csv"

# Load test dataset
test_data = spark.read.csv(f"{hdfs_path}test_stock_data.csv", header=True, inferSchema=True)

# Identify target stock close price columns
target_cols = [f"{col}_Close" for col in ["AAPL", "MSFT", "GOOGL", "TSLA", "AMZN", "NVDA", "JPM", "V", "XOM"]]

# Forecast methods and their paths
forecast_methods = {
    "LSTM": forecast_lstm_path,
    "ARIMA": forecast_arima_path,
    "VAR": forecast_normal_path
}

metrics_list = []

for method, path in forecast_methods.items():
    print(f"\n--- Evaluating {method} Forecast ---")
    
    for target_col in target_cols:
        predictions_path = f"{path}{target_col}_predictions.csv"

        try:
            predictions = spark.read.csv(predictions_path, header=True, inferSchema=True)
        except Exception as e:
            print(f"Error loading predictions for {target_col} ({method}): {e}")
            continue
        
        # Select relevant columns
        test_data_actual = test_data.select("Date", target_col)
        predictions = predictions.select("Date", "Prediction").withColumnRenamed("Prediction", "Prediction_Value")
        
        # Join actual and predicted data
        results = test_data_actual.join(predictions, on="Date", how="inner")
        results = results.withColumnRenamed(target_col, "Actual_Value")

        # Initialize evaluators
        evaluator_mse = RegressionEvaluator(labelCol="Actual_Value", predictionCol="Prediction_Value", metricName="mse")
        evaluator_rmse = RegressionEvaluator(labelCol="Actual_Value", predictionCol="Prediction_Value", metricName="rmse")
        evaluator_mae = RegressionEvaluator(labelCol="Actual_Value", predictionCol="Prediction_Value", metricName="mae")
        evaluator_r2 = RegressionEvaluator(labelCol="Actual_Value", predictionCol="Prediction_Value", metricName="r2")

        # Compute metrics
        mse = evaluator_mse.evaluate(results)
        rmse = evaluator_rmse.evaluate(results)
        mae = evaluator_mae.evaluate(results)
        r2 = evaluator_r2.evaluate(results)

        print(f"{method} - {target_col}: MSE={mse}, RMSE={rmse}, MAE={mae}, R2={r2}")
        metrics_list.append((method, target_col, mse, rmse, mae, r2))

# Save all metrics to a single CSV file
metrics_df = spark.createDataFrame(metrics_list, ["Method", "Stock", "MSE", "RMSE", "MAE", "R2"])
metrics_df.write.csv(final_output_path, header=True, mode="overwrite")

print(f"All evaluation metrics saved to {final_output_path}")
