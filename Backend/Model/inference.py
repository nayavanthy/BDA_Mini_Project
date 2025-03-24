from pyspark.sql import SparkSession
from pyspark.ml.evaluation import RegressionEvaluator

# Initialize Spark session
spark = SparkSession.builder.appName("StockEvaluation").getOrCreate()

hdfs_path = "hdfs://localhost:9000/user/bda_mini_project/inferred_data/"
forecast_path = "hdfs://localhost:9000/user/bda_mini_project/forecast/"

# Load test dataset
test_data = spark.read.csv(f"{hdfs_path}test_stock_data.csv", header=True, inferSchema=True)

target_cols = [f"{col}_Close" for col in ["AAPL", "MSFT", "GOOGL", "TSLA", "AMZN", "NVDA", "JPM", "V", "XOM"]]

metrics_list = []

for target_col in target_cols:
    predictions_path = f"{forecast_path}{target_col}_predictions.csv"
    predictions = spark.read.csv(predictions_path, header=True, inferSchema=True)
    
    # Select relevant columns
    test_data_actual = test_data.select("Date", target_col)
    predictions = predictions.select("Date", "prediction").withColumnRenamed("prediction", f"{target_col}_prediction")
    
    # Join actual and predicted data
    results = test_data_actual.join(predictions, on="Date", how="inner")
    results = results.withColumnRenamed(target_col, "label")
    
    # Initialize evaluators
    evaluator_mse = RegressionEvaluator(labelCol="label", predictionCol=f"{target_col}_prediction", metricName="mse")
    evaluator_rmse = RegressionEvaluator(labelCol="label", predictionCol=f"{target_col}_prediction", metricName="rmse")
    evaluator_mae = RegressionEvaluator(labelCol="label", predictionCol=f"{target_col}_prediction", metricName="mae")
    evaluator_r2 = RegressionEvaluator(labelCol="label", predictionCol=f"{target_col}_prediction", metricName="r2")
    
    # Compute metrics
    mse = evaluator_mse.evaluate(results)
    rmse = evaluator_rmse.evaluate(results)
    mae = evaluator_mae.evaluate(results)
    r2 = evaluator_r2.evaluate(results)
    
    print(f"Evaluation for {target_col}: MSE={mse}, RMSE={rmse}, MAE={mae}, R2={r2}")
    metrics_list.append((target_col, mse, rmse, mae, r2))

# Save all metrics to a single CSV
metrics_df = spark.createDataFrame(metrics_list, ["Stock", "MSE", "RMSE", "MAE", "R2"])
metrics_output_path = f"{forecast_path}evaluation_metrics.csv"
metrics_df.write.csv(metrics_output_path, header=True, mode="overwrite")

print(f"All evaluation metrics saved to {metrics_output_path}")
