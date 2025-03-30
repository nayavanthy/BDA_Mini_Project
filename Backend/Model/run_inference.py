import subprocess
import pandas as pd
import matplotlib.pyplot as plt
import json
import sys
import os
import glob

# Add the Backend directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Now import the run module
from DashBoard import run

# Run inference script using Spark
file = "/home/captain/Desktop/BDA/Backend/Model/inference.py"
subprocess.run(f"sudo -u hadoop /home/hadoop/spark/bin/spark-submit --master yarn --deploy-mode client {file}", shell=True)

# Run the dashboard script
run.run()

def find_and_merge_part_files(directory):
    """Finds and merges all part-*.csv files into a single DataFrame."""
    files = sorted(glob.glob(os.path.join(directory, "part-*.csv")))  # Get all matching files
    if not files:
        return None  # Return None if no files found

    df_list = [pd.read_csv(file) for file in files]  # Read each file
    merged_df = pd.concat(df_list, ignore_index=True)  # Merge into one DataFrame
    
    return merged_df  # Return the merged DataFrame

# Define base directory
base_dir = "/home/captain/Desktop/BDA/Backend/DashBoard/evaluation_metrics.csv"

# Load and merge evaluation metrics
metrics_df = find_and_merge_part_files(base_dir)

if metrics_df is None:
    print(json.dumps({"error": "No evaluation metrics found."}), flush=True)
    sys.exit()

# Directory to save plots
plot_dir = "/home/captain/Desktop/BDA/Backend/DashBoard/method_plots/"
os.makedirs(plot_dir, exist_ok=True)

# Metrics to plot
metrics = ["MSE", "RMSE", "MAE", "R2"]
colors = ["blue", "green", "red", "purple"]
plot_paths = {}

# Generate individual plots for each method
for method in metrics_df["Method"].unique():
    fig, ax = plt.subplots(figsize=(12, 6))

    subset = metrics_df[metrics_df["Method"] == method]

    # Plot each metric
    for metric, color in zip(metrics, colors):
        ax.plot(subset["Stock"], subset[metric], label=metric, marker="o", linestyle="-", color=color)

    # Formatting
    ax.set_title(f"Performance of {method}")
    ax.set_xlabel("Stock")
    ax.set_ylabel("Metric Value")
    ax.legend()
    ax.grid(True)
    plt.xticks(rotation=45)

    # Save plot
    plot_path = os.path.join(plot_dir, f"{method}_evaluation_plot.png")
    plt.savefig(plot_path, format="png", bbox_inches="tight")
    plt.close()

    plot_paths[method] = plot_path

# Return JSON with all plot paths
output = {"plot_paths": plot_paths}
print(json.dumps(output), flush=True)
