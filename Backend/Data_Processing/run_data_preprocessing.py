import subprocess
import os

file = "/home/captain/Desktop/BDA/Backend/Data_Processing/Data_Preprocessing.py"

subprocess.run(f"sudo -u hadoop /home/hadoop/spark/bin/spark-submit --master yarn --deploy-mode client {file}", shell=True)

file = "/home/captain/Desktop/BDA/Backend/Data_Processing/Data_Preprocessing.py"

subprocess.run(f"sudo -u hadoop /home/hadoop/spark/bin/spark-submit --master yarn --deploy-mode client {file}", shell=True)