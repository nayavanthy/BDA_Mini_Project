import subprocess

# HDFS directory where CSVs are stored
hdfs_dir = "/user/bda_mini_project/data/stocks_data.csv"
hive_db = "stock_data_db"  # Change if needed
hive_table = "stock_prices"

# Start Hadoop services if needed (optional, ensure services are running)
subprocess.run("sbin/start-dfs.sh", shell=True)
subprocess.run("sbin/start-yarn.sh", shell=True)
subprocess.run("hive --service metastore &", shell=True)

# Create Hive database if it doesn’t exist
create_db_query = f"CREATE DATABASE IF NOT EXISTS {hive_db};"

# Create External Hive Table (if not exists)
create_table_query = f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {hive_db}.{hive_table} (
    date STRING,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume INT
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE 
LOCATION '{hdfs_dir}';
"""

# Execute Hive Queries
commands = [
    f"hive -e \"{create_db_query}\"",
    f"hive -e \"{create_table_query}\""
]

for cmd in commands:
    subprocess.run(cmd, shell=True)

print(f"✅ Successfully loaded CSV files into Hive table `{hive_db}.{hive_table}`")
