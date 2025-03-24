from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder.appName("StockForecasting").getOrCreate()

hdfs_path = "hdfs://localhost:9000/user/bda_mini_project" 
in_data = "/data/"
out_data = "/inferred_data/"

# Load Stock Data
stock_data = spark.read.csv(f"{hdfs_path}{in_data}stocks_data.csv", header=True, inferSchema=True)

# Drop the first row
stock_data = stock_data.rdd.zipWithIndex() \
    .filter(lambda row: row[1] > 0) \
    .map(lambda row: row[0]) \
    .toDF(stock_data.schema)

# Load Economic Indicator Data
economic_dfs = {}
indicators = ["SP_500", "NASDAQ", "Dow_Jones", "10Y_Treasury_Yield", "Gold_Prices", "Oil_Prices"]

for indicator in indicators:
    path = f"{hdfs_path}{in_data}{indicator}.csv" 
    df = spark.read.csv(path, header=True, inferSchema=True)
    df = df.drop("Ticker")
    df = df.withColumnRenamed("Close", f"{indicator}_Close") \
           .withColumnRenamed("High", f"{indicator}_High") \
           .withColumnRenamed("Low", f"{indicator}_Low") \
           .withColumnRenamed("Open", f"{indicator}_Open") \
           .withColumnRenamed("Volume", f"{indicator}_Volume")
    economic_dfs[indicator] = df

# Merge all economic indicators on Date
economic_data = economic_dfs[indicators[0]]
for indicator in indicators[1:]:
    economic_data = economic_data.join(economic_dfs[indicator], on="Date", how="outer")

# Merge stock data with economic data
merged_data = stock_data.join(economic_data, on="Date", how="outer")

# Handle Missing Values (Forward Fill Approximation)
window_spec = Window.orderBy("Date").rowsBetween(-1, 0)
for column in merged_data.columns:
    if column != "Date":
        merged_data = merged_data.withColumn(column, avg(col(column)).over(window_spec))

# Normalize Data (Min-Max Scaling)
def min_max_scaling(df, feature_cols):
    for col_name in feature_cols:
        min_val = df.agg({col_name: "min"}).collect()[0][0]
        max_val = df.agg({col_name: "max"}).collect()[0][0]
        df = df.withColumn(col_name, (col(col_name) - min_val) / (max_val - min_val))
    return df

feature_columns = [col for col in merged_data.columns if col != "Date"]
normalized_data = min_max_scaling(merged_data, feature_columns)

# Save Processed Data
normalized_data.write.csv(f"{hdfs_path}{out_data}processed_stock_data.csv", header=True, mode="overwrite")

# Show Sample Data
normalized_data.show(5)
