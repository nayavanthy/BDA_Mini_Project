import pandas as pd
import numpy as np
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Dropout
from sklearn.preprocessing import MinMaxScaler
from sklearn.model_selection import train_test_split
import matplotlib.pyplot as plt

# Load datasets
def load_data(file_path):
    return pd.read_csv(file_path, parse_dates=['Date'], index_col='Date')

data_in = "/home/captain/Desktop/BDA/Backend/Data_Collection/"
# Load stock data & external factors
stock_data = load_data(f'{data_in}stocks_data.csv')
treasury = load_data(f'{data_in}10Y_Treasury_Yield.csv')

treasury = treasury.drop('Ticker', axis=1)

# Keep only AAPL_close from stock_data
stock_data = stock_data[['AAPL_Close']]

# Merge all datasets on Date
all_data = stock_data.join([treasury], how='inner')
all_data.dropna(inplace=True)

# Select features and target variable
target_col = 'AAPL_Close'
features = [col for col in all_data.columns if col != target_col]

# Scale data
scaler = MinMaxScaler()
scaled_data = scaler.fit_transform(all_data)
scaled_df = pd.DataFrame(scaled_data, columns=all_data.columns, index=all_data.index)

# Create sequences for LSTM
def create_sequences(data, target_col, seq_length=60):
    X, Y = [], []
    for i in range(len(data) - seq_length):
        X.append(data.iloc[i:i+seq_length].values)
        Y.append(data.iloc[i+seq_length][target_col])
    return np.array(X), np.array(Y)

seq_length = 60  # Use past 60 days to predict the next day
X, Y = create_sequences(scaled_df, target_col, seq_length)

# Split into train & test sets
X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size=0.2, shuffle=False)

# Build LSTM model
model = Sequential([
    LSTM(50, return_sequences=True, input_shape=(seq_length, X.shape[2])),
    Dropout(0.2),
    LSTM(50, return_sequences=False),
    Dropout(0.2),
    Dense(25, activation='relu'),
    Dense(1)
])

# Compile model
model.compile(optimizer='adam', loss='mse')

# Train model
epochs = 20
batch_size = 32
history = model.fit(X_train, Y_train, validation_data=(X_test, Y_test), epochs=epochs, batch_size=batch_size)

# Predict & inverse scale
predictions = model.predict(X_test)

# Plot results
plt.figure(figsize=(12,6))
plt.plot(all_data.index[-len(Y_test):], Y_test, label='Actual Prices')
plt.plot(all_data.index[-len(predictions):], predictions, label='Predicted Prices')
plt.legend()
plt.show()
