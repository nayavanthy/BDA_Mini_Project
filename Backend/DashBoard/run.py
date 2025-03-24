import subprocess
import os

def run():
    file = "/home/captain/Desktop/BDA/Backend/DashBoard/get_data.py"

    subprocess.run(f"sudo -u hadoop python3 {file}", shell=True)