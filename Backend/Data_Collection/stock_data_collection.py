import yfinance as yf
import pandas as pd

def run():
    # Define a list of stock symbols
    stocks = ["AAPL", "MSFT", "GOOGL", "TSLA", "AMZN", "NVDA", "JPM", "V", "XOM"]

    # Set the date range
    start_date = "2015-01-01"
    end_date = "2024-01-01"

    # Create an empty dictionary to store individual stock DataFrames
    stock_data = {}

    # Fetch data for each stock
    for stock in stocks:
        df = yf.download(stock, start=start_date, end=end_date)
        
        # Rename columns to include stock symbol
        df = df.add_prefix(f"{stock}_")  
        stock_data[stock] = df

    # Merge all stock data on the date index
    all_stocks_df = pd.concat(stock_data.values(), axis=1)

    # Fill missing values with forward-fill (optional)
    all_stocks_df.ffill(inplace=True)

    # Reset index and save the dataset
    all_stocks_df.reset_index(inplace=True)
    all_stocks_df.to_csv("/home/captain/Desktop/BDA/Backend/Data_Collection/stocks_data.csv", index=False)

    print("Stock data collected successfully!")
