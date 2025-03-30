import yfinance as yf
import pandas as pd

import stock_data_collection

start_date = "2015-01-01"
end_date = "2024-01-01"

# Economic indicators from Yahoo Finance
indicators = {
    "S&P_500": "^GSPC",
    "NASDAQ": "^IXIC",
    "Dow_Jones": "^DJI",
    "10Y_Treasury_Yield": "^TNX",
    "Gold_Prices": "GC=F",
    "Oil_Prices": "CL=F",
}

# Fetch and save each economic indicator separately
for name, ticker in indicators.items():
    print(f"Fetching data for {name} ({ticker})...")
    
    # Download data
    df = yf.download(ticker, start=start_date, end=end_date)
    
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0) 

    print(df.head())

    # Insert 'Ticker' column for reference
    df.insert(0, "Ticker", ticker)
    
    # Reset index so 'Date' becomes a column
    df.reset_index(inplace=True)

    # Save to CSV
    if name == "S&P_500":
        filename = f"/home/captain/Desktop/BDA/Backend/Data_Collection/SP_500.csv"
    else:
        filename = f"/home/captain/Desktop/BDA/Backend/Data_Collection/{name}.csv"
    df.to_csv(filename, index=False)  # Save properly formatted file
    
    print(f"Saved: {filename}")

print("Economic indicator collection complete.")

stock_data_collection.run()