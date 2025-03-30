import subprocess

# Define local directory where CSVs will be stored
LOCAL_DIR = "/home/captain/Desktop/BDA/Backend/DashBoard/"

# List of HDFS file paths to download
HDFS_FILES = ["/user/bda_mini_project/evaluation_metrics.csv"]

# Command template for downloading files from HDFS
HDFS_GET_CMD = "~/hadoop/bin/hdfs dfs -get -f {hdfs_file} {local_dir}"

# Download each file
for hdfs_file in HDFS_FILES:
    subprocess.run(HDFS_GET_CMD.format(hdfs_file=hdfs_file, local_dir=LOCAL_DIR), shell=True)
    print(f"✅ Downloaded {hdfs_file} to {LOCAL_DIR}")

print("✅ All CSV files downloaded successfully!")
