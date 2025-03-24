import pandas as pd

stocks = ["AAPL", "MSFT", "GOOGL", "TSLA", "AMZN", "NVDA", "JPM", "V", "XOM"]

# Load data
stocks_df = pd.read_csv("stocks_data.csv")
econ_df = pd.read_csv("economic_indicators.csv")

# Display sample
print(stocks_df.head())
print(econ_df.head())

# Check for missing values
print(stocks_df.isnull().sum())
print(econ_df.isnull().sum())

# Summary statistics
print(stocks_df.describe())
print(econ_df.describe())

# Plot stock prices for quick visualization
import matplotlib.pyplot as plt

for stock in stocks[:3]:  # Plot first 5 stocks
    stock_data = stocks_df[stocks_df["Stock"] == stock]
    plt.plot(stock_data["Date"], stock_data["Close"], label=stock)

plt.legend()
plt.title("Stock Price Trends")
plt.xlabel("Date")
plt.ylabel("Closing Price")
plt.xticks(rotation=45)
plt.show()
