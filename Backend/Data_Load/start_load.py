import subprocess
import os

file = "/home/captain/Desktop/BDA/Backend/Data_Load/hdfs_load.py"

subprocess.run(f"sudo -u hadoop python3 {file}", shell=True)
