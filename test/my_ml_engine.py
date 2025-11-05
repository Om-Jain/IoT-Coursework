import os
import pandas as pd
from prophet import Prophet
import matplotlib.pyplot as plt


class MLPredictor(object):
    """
    Example usage method:

        predictor = MLPredictor("pm25_daily_averages.csv")
        predictor.train()
        forecast = predictor.predict()
        fig = predictor.plot_result(forecast)
        fig.savefig("forecast_plot.png")
    """

    def __init__(self, csv_path):
        """
        :param csv_path: Path to pm25_daily_averages.csv
        """
        self.__train_data = self.__load_and_prepare(csv_path)
        self.__trainer = Prophet(changepoint_prior_scale=12)

    def __load_and_prepare(self, csv_path):
        # Read CSV file
        df = pd.read_csv(csv_path)

        # Try to automatically detect columns
        if "Date" in df.columns:
            df.rename(columns={"Date": "Timestamp"}, inplace=True)
        if "Daily_Avg_PM2.5" in df.columns:
            df.rename(columns={"Daily_Avg_PM2.5": "Value"}, inplace=True)

        # Convert columns to Prophet format
        df = self.__convert_col_name(df)

        # Ensure datetime conversion
        df["ds"] = pd.to_datetime(df["ds"])

        print(f"\n✅ Loaded {len(df)} records from {csv_path}")
        print(df.head())
        return df

    def train(self):
        print("\n🚀 Training the Prophet model...")
        self.__trainer.fit(self.__train_data)
        print("✅ Model training completed.")

    def __convert_col_name(self, data_df):
        # Rename columns based on possible names
        rename_map = {}
        for col in data_df.columns:
            col_lower = col.lower().strip()
            if col_lower in ["date", "timestamp", "datetime", "time"]:
                rename_map[col] = "ds"
            elif col_lower in ["average", "avg", "value", "pm2.5", "pm25"]:
                rename_map[col] = "y"
        data_df.rename(columns=rename_map, inplace=True)

        # Check if Prophet-required columns exist
        if not {"ds", "y"}.issubset(data_df.columns):
            raise ValueError(
                f"❌ Missing required columns for Prophet. "
                f"Expected ['ds', 'y'], found: {data_df.columns.tolist()}"
            )

        print(f"✅ After rename columns: {data_df.columns.tolist()}")
        return data_df

    def __make_future(self, periods=15):
        future = self.__trainer.make_future_dataframe(periods=periods)
        return future

    def predict(self):
        print("\n🔮 Making predictions for future days...")
        future = self.__make_future()
        forecast = self.__trainer.predict(future)
        print("✅ Forecasting completed.")
        return forecast

    def plot_result(self, forecast):
        fig = self.__trainer.plot(forecast, figsize=(15, 6))
        plt.title("PM2.5 Forecast")
        plt.xlabel("Date")
        plt.ylabel("PM2.5 Level")
        plt.grid(True)
        plt.tight_layout()
        return fig


# --- Run the predictor ---
if __name__ == "__main__":
    csv_file = "pm25_daily_averages.csv"  # Path to your CSV
    predictor = MLPredictor(csv_file)
    predictor.train()
    forecast = predictor.predict()
    fig = predictor.plot_result(forecast)

    # Save the plot
    output_path = os.path.join(os.getcwd(), "pm25_forecast.png")
    fig.savefig(output_path)
    print(f"\n📊 Forecast plot saved at: {output_path}")
