import subprocess
import pandas as pd
import matplotlib.pyplot as plt
import io
import base64
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
# subprocess.run(f"sudo -u hadoop /home/hadoop/spark/bin/spark-submit --master yarn --deploy-mode client {file}", shell=True)

# Run the dashboard script
run.run()

# Function to find the correct part-0000*.csv file in a given directory
def find_part_file(directory):
    files = glob.glob(os.path.join(directory, "part-0000*.csv"))
    return files[0] if files else None  # Return the first (and only) matching file

# Define base directory
base_dir = "/home/captain/Desktop/BDA/Backend/DashBoard"

# Load datasets dynamically
test_file = find_part_file(os.path.join(base_dir, "test_stock_data.csv"))
test = pd.read_csv(test_file) if test_file else None

stock_dirs = {
    "AAPL": "AAPL_Close_predictions.csv",
    "AMZN": "AMZN_Close_predictions.csv",
    "GOOGL": "GOOGL_Close_predictions.csv",
    "JPM": "JPM_Close_predictions.csv",
    "MSFT": "MSFT_Close_predictions.csv",
    "NVDA": "NVDA_Close_predictions.csv",
    "TSLA": "TSLA_Close_predictions.csv",
    "V": "V_Close_predictions.csv",
    "XOM": "XOM_Close_predictions.csv",
}

stocks = {}

for symbol, dir_name in stock_dirs.items():
    stock_file = find_part_file(os.path.join(base_dir, dir_name))
    if stock_file:
        stocks[symbol] = pd.read_csv(stock_file)

# Create a multi-plot figure
fig, axes = plt.subplots(nrows=3, ncols=3, figsize=(15, 12))
fig.suptitle("Stock Prices: Actual vs. Predicted", fontsize=16)

# Flatten the axes array for easy iteration
axes = axes.flatten()

# Plot each stock
for i, (symbol, df) in enumerate(stocks.items()):
    ax = axes[i]
    ax.plot(test['Date'], test[f"{symbol}_Close"], label=f"Actual {symbol}", color="blue")
    ax.plot(df['Date'], df['prediction'], label=f"Predicted {symbol}", linestyle="dashed", color="red")
    ax.set_title(symbol)
    ax.legend()
    ax.tick_params(axis="x", rotation=45)
    ax.grid(True)

# Adjust layout
plt.tight_layout(rect=[0, 0, 1, 0.96])

# Define a path for the saved plot
plot_path = "/home/captain/Desktop/BDA/Backend/DashBoard/stock_plot.png"

# Save the plot
plt.savefig(plot_path, format="png", bbox_inches="tight")

# Return JSON with file path instead of Base64
output = {"plot_path": plot_path}
print(json.dumps(output), flush=True)  # Ensure it's printed cleanly