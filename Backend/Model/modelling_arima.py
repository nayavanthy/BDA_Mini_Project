from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from statsmodels.tsa.arima.model import ARIMA
from pmdarima import auto_arima

# Initialize Spark session
spark = SparkSession.builder.appName("ARIMA_Stock_Forecasting").getOrCreate()

# HDFS path
hdfs_path = "hdfs://localhost:9000/user/bda_mini_project/inferred_data/"

# Load dataset
data = spark.read.csv(f"{hdfs_path}train_stock_data.csv", header=True, inferSchema=True)
test_data = spark.read.csv(f"{hdfs_path}test_stock_data.csv", header=True, inferSchema=True)

# Convert test data to Pandas for row count
test_data_pd = test_data.toPandas()
test_length = len(test_data_pd)  # Number of test days

# Identify stock close price columns
target_cols = [f"{col}_Close" for col in ["AAPL", "MSFT", "GOOGL", "TSLA", "AMZN", "NVDA", "JPM", "V", "XOM"]]

for target_col in target_cols:
    print(f"\n--- Training for {target_col} ---")

    # Convert training data to Pandas
    df = data.toPandas()

    # Keep only Date and the target stock price column
    df = df[["Date", target_col]]

    # Convert Date column to datetime and set as index
    df['Date'] = pd.to_datetime(df['Date'])
    df.set_index('Date', inplace=True)

    # Ensure daily frequency
    df = df.asfreq("D")
    df.fillna(method="ffill", inplace=True)  # Forward-fill missing values

    # Fit ARIMA model using auto_arima to find best parameters
    stepwise_fit = auto_arima(df[target_col], seasonal=False, trace=True, suppress_warnings=True)

    # Extract best ARIMA order
    p, d, q = stepwise_fit.order
    print(f"Optimal ARIMA order for {target_col}: (p={p}, d={d}, q={q})")

    # Train ARIMA model
    model = ARIMA(df[target_col], order=(p, d, q))
    model_fit = model.fit()
    print(model_fit.summary())

    # Forecast next test_length days
    forecast = model_fit.forecast(steps=test_length)

    # Create DataFrame for predictions
    future_dates = pd.date_range(start=df.index[-1] + pd.Timedelta(days=1), periods=test_length, freq="D")
    predictions_df = pd.DataFrame({"Date": future_dates, "Prediction": forecast})

    # Convert to Spark DataFrame
    predictions_spark = spark.createDataFrame(predictions_df)

    # Save to HDFS
    output_path = f"hdfs://localhost:9000/user/bda_mini_project/forecast_arima/{target_col}_predictions.csv"
    predictions_spark.write.csv(output_path, header=True, mode="overwrite")

    print(f"Predictions for {target_col} saved to {output_path}")
