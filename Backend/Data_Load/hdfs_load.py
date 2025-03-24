import subprocess
import os

# Define local directory where CSVs are stored
local_dir = "/home/captain/Desktop/BDA/Backend/Data_Collection"

# Define HDFS directory
hdfs_dir = "/user/bda_mini_project/data/"

subprocess.run(f"~/hadoop/bin/hdfs dfs -mkdir -p {hdfs_dir}", shell=True)

csv_files = [f for f in os.listdir(local_dir) if f.endswith(".csv")]

for file in csv_files:
    local_path = os.path.join(local_dir, file)
    hdfs_path = hdfs_dir + file
    subprocess.run(f"~/hadoop/bin/hdfs dfs -put -f {local_path} {hdfs_path}", shell=True)
    print(f"Uploaded {file} to HDFS at {hdfs_path}")

print("✅ All CSV files uploaded successfully!")
