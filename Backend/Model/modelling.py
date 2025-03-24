from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

# Initialize Spark session
spark = SparkSession.builder.appName("VAR_Stock_Forecasting").getOrCreate()

hdfs_path = "hdfs://localhost:9000/user/bda_mini_project/inferred_data/"

# Load processed dataset
data = spark.read.csv(f"{hdfs_path}train_stock_data.csv", header=True, inferSchema=True)

# Identify all stock close price columns
target_cols = [f"{col}_Close" for col in ["AAPL", "MSFT", "GOOGL", "TSLA", "AMZN", "NVDA", "JPM", "V", "XOM"]]

for target_col in target_cols:
    # Define feature columns (excluding 'Date', target column, and any unwanted columns)
    feature_cols = [col for col in data.columns if col not in ["Date", target_col, "10Y_Treasury_Yield_Volume"]]
    
    # Assemble features into a single vector column
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    prepared_data = assembler.transform(data).select("Date", "features", target_col)
    
    # Define and train linear regression model
    lr = LinearRegression(featuresCol="features", labelCol=target_col)
    model = lr.fit(prepared_data)
    
    # Load test data
    test_data = spark.read.csv(f"{hdfs_path}test_stock_data.csv", header=True, inferSchema=True)
    
    # Transform test data
    test_data = assembler.transform(test_data).select("Date", "features")
    
    # Predict on test data
    predictions = model.transform(test_data)
    predictions = predictions.select("Date", "prediction")
    
    # Save predictions to CSV
    output_path = f"hdfs://localhost:9000/user/bda_mini_project/forecast/{target_col}_predictions.csv"
    predictions.write.csv(output_path, header=True, mode="overwrite")
    
    print(f"Predictions for {target_col} saved to {output_path}")
