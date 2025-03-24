from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("StockForecasting").getOrCreate()

hdfs_path = "hdfs://localhost:9000/user/bda_mini_project/inferred_data/"

# Load Stock Data
stock_data = spark.read.csv(f"{hdfs_path}processed_stock_data.csv", header=True, inferSchema=True)

total_rows = stock_data.count()
train_rows = int(total_rows * 0.8)

train_data = stock_data.limit(train_rows)
test_data = stock_data.subtract(train_data)

train_data.write.csv(f"{hdfs_path}train_stock_data.csv", header=True, mode="overwrite")
test_data.write.csv(f"{hdfs_path}test_stock_data.csv", header=True, mode="overwrite")

print("Done")