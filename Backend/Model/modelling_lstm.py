from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
import pandas as pd
import numpy as np
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Dropout

# Initialize Spark session
spark = SparkSession.builder.appName("LSTM_Stock_Forecasting").getOrCreate()

# HDFS path
hdfs_path = "hdfs://localhost:9000/user/bda_mini_project/inferred_data/"

# Load dataset
data = spark.read.csv(f"{hdfs_path}train_stock_data.csv", header=True, inferSchema=True)

# Identify stock close price columns
target_cols = [f"{col}_Close" for col in ["AAPL", "MSFT", "GOOGL", "TSLA", "AMZN", "NVDA", "JPM", "V", "XOM"]]

def create_sequences(data, target_col, seq_length=60):
    """Creates input sequences for LSTM, excluding target_col from X"""
    X, Y = [], []
    feature_cols = [col for col in data.columns if col != target_col]  # Exclude target_col
    
    for i in range(len(data) - seq_length):
        X.append(data[feature_cols].iloc[i:i+seq_length].values)  # Use only feature columns
        Y.append(data.iloc[i+seq_length][target_col])  # Target remains the same
    
    return np.array(X), np.array(Y)

for target_col in target_cols:
    print(f"\n--- Training for {target_col} ---")
    
    # Define feature columns
    feature_cols = [col for col in data.columns if col not in ["Date", target_col, "10Y_Treasury_Yield_Volume"]]

    # Assemble features
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    all_data = assembler.transform(data).select("features", target_col)

    # Convert to Pandas DataFrame
    all_data_df = all_data.toPandas()
    print(all_data_df.columns)

    # Convert DenseVector to NumPy array
    all_data_df["features"] = all_data_df["features"].apply(lambda x: np.array(x.toArray()))

    # Convert list of arrays into a proper NumPy array
    feature_matrix = np.vstack(all_data_df["features"].values)

    # Create DataFrame with expanded features
    feature_df = pd.DataFrame(feature_matrix, columns=feature_cols)

    # Combine with target column
    all_data_df = pd.concat([feature_df, all_data_df[target_col]], axis=1)

    # Create sequences
    seq_length = 60
    X, Y = create_sequences(all_data_df, target_col, seq_length)

    # Define LSTM model
    model = Sequential([
        LSTM(50, return_sequences=True, input_shape=(seq_length, X.shape[2])),
        Dropout(0.2),
        LSTM(50, return_sequences=False),
        Dropout(0.2),
        Dense(25, activation='relu'),
        Dense(1)
    ])

    model.compile(optimizer='adam', loss='mse')

    # Train model
    epochs = 20
    batch_size = 32
    history = model.fit(X, Y, validation_split=0.2, epochs=epochs, batch_size=batch_size)

    # Load test data
    test_data = spark.read.csv(f"{hdfs_path}test_stock_data.csv", header=True, inferSchema=True)

    # Use same feature set as training
    test_assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    test_data = test_assembler.transform(test_data).select('Date',"features",target_col)

    # Convert to Pandas
    test_data_df = test_data.toPandas()
    print(test_data_df.columns)
    # Extract Date column for final predictions
    test_dates = test_data_df["Date"]
    
    test_data_df = test_data_df.drop('Date', axis = 1)

    # Convert DenseVector to NumPy
    test_data_df["features"] = test_data_df["features"].apply(lambda x: np.array(x.toArray()))

    # Convert list of arrays into a proper NumPy array
    test_features = np.vstack(test_data_df["features"].values)

    # Create sequences
    X_test = np.array([test_features[i:i+seq_length] for i in range(len(test_features) - seq_length)])

    # Make predictions
    predictions = model.predict(X_test)

    # Save predictions
    predictions_df = pd.DataFrame({"Date": test_dates.iloc[seq_length:].values, "prediction": predictions.flatten()})
    
    # Convert to Spark DataFrame
    predictions_spark = spark.createDataFrame(predictions_df)

    # Save to HDFS
    output_path = f"hdfs://localhost:9000/user/bda_mini_project/forecast_lstm/{target_col}_predictions.csv"
    predictions_spark.write.csv(output_path, header=True, mode="overwrite")
    
    print(f"Predictions for {target_col} saved to {output_path}")
