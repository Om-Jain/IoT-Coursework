import pandas as pd
from my_ml_engine import MLPredictor   # renamed from ml_engine.py → my_ml_engine.py

if __name__ == '__main__':
    # Load PM2.5 data from CSV
    csv_file = "pm25_daily_averages.csv"  # ensure this file is in the same folder
    data_df = pd.read_csv(csv_file)

    # Rename columns to match what MLPredictor expects
    if "date" in data_df.columns and "average" in data_df.columns:
        data_df.rename(columns={"date": "Timestamp", "average": "Value"}, inplace=True)

    print(f"✅ Loaded {len(data_df)} records from {csv_file}")
    print(data_df.head())

    # Create the MLPredictor object
    predictor = MLPredictor(data_df)

    # Train Prophet model
    predictor.train()

    # Predict future PM2.5 values
    forecast = predictor.predict()

    # Plot and save the result
    fig = predictor.plot_result(forecast)
    fig.savefig("pm25_prediction.png")
    fig.show()
