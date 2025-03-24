import subprocess
import os

file = "/home/captain/Desktop/BDA/Backend/Model/modelling.py"

subprocess.run(f"sudo -u hadoop /home/hadoop/spark/bin/spark-submit --master yarn --deploy-mode client {file}", shell=True)