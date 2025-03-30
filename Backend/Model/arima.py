import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from statsmodels.tsa.arima.model import ARIMA
from pmdarima import auto_arima

data_in = "/home/captain/Desktop/BDA/Backend/Data_Collection/"
# Load stock data & external factors
stock_data = pd.read_csv(f'{data_in}stocks_data.csv')
# Convert 'date' column to datetime
stock_data['Date'] = pd.to_datetime(stock_data['Date'])
stock_data.set_index('Date', inplace=True)
print(stock_data.head)
df = stock_data.asfreq("D")  # Ensure daily frequency
df.fillna(method="ffill", inplace=True) 

stepwise_fit = auto_arima(df["AAPL_Close"], seasonal=False, trace=True, suppress_warnings=True)
print(stepwise_fit.summary())  # This suggests the best (p, d, q) values

p, d, q = stepwise_fit.order
model = ARIMA(df["AAPL_Close"], order=(p, d, q))
model_fit = model.fit()
print(model_fit.summary())

forecast_steps = 30  # Predict next 30 days
forecast = model_fit.forecast(steps=forecast_steps)

# Plot results
plt.figure(figsize=(10, 5))
plt.plot(df.index, df["AAPL_Close"], label="Actual")
plt.plot(pd.date_range(df.index[-1], periods=forecast_steps+1, freq="D")[1:], forecast, label="Forecast", linestyle="dashed")
plt.legend()
plt.show()

