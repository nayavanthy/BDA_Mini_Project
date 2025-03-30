import subprocess
import os

file = "/home/captain/Desktop/BDA/Backend/Model/modelling_lstm.py"
conda_python = "/home/captain/miniconda3/envs/tf_gpu/bin/python"
subprocess.run(f"sudo -u hadoop PYSPARK_PYTHON={conda_python} PYSPARK_DRIVER_PYTHON={conda_python} /home/hadoop/spark/bin/spark-submit --master yarn --deploy-mode client {file}", shell=True) 