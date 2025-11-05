# # import requests
# # import pandas as pd
# # from datetime import datetime
# #
# # # --- Configuration ---
# # API_URL = "https://raw.githubusercontent.com/ncl-iot-team/CSC8112/main/data/uo_data.min.json"
# # OUTPUT_FILE = "pm25_data.xlsx"
# #
# # # --- Function to Fetch and Extract PM2.5 Data ---
# # def fetch_pm25_data():
# #     response = requests.get(API_URL)
# #     response.raise_for_status()
# #     data = response.json()
# #
# #     pm25_records = []
# #
# #     for sensor in data.get("sensors", []):
# #         sensor_data = sensor.get("data")
# #         if not isinstance(sensor_data, dict):
# #             continue
# #
# #         if "PM2.5" in sensor_data:
# #             for entry in sensor_data["PM2.5"]:
# #                 ts = entry.get("Timestamp")
# #                 val = entry.get("Value")
# #
# #                 if ts and val is not None:
# #                     # Convert timestamp (milliseconds → datetime)
# #                     ts_dt = datetime.utcfromtimestamp(ts / 1000)
# #                     pm25_records.append({
# #                         "Timestamp": ts_dt,
# #                         "Date": ts_dt.date(),
# #                         "Pollutant": "PM2.5",
# #                         "Value": val
# #                     })
# #
# #     return pm25_records
# #
# # # --- Function to Compute Daily Average ---
# # def compute_daily_average(df):
# #     daily_avg = df.groupby("Date")["Value"].mean().reset_index()
# #     daily_avg.rename(columns={"Value": "Daily_Avg_PM2.5"}, inplace=True)
# #     return daily_avg
# #
# # # --- Save to Excel (Raw + Daily Average) ---
# # def save_to_excel(raw_df, avg_df, filename):
# #     with pd.ExcelWriter(filename, engine='openpyxl') as writer:
# #         raw_df.to_excel(writer, index=False, sheet_name="Raw_PM2.5_Data")
# #         avg_df.to_excel(writer, index=False, sheet_name="Daily_Average_PM2.5")
# #     print(f"✅ Data saved successfully to '{filename}'")
# #     print(f"📊 Total Records: {len(raw_df)} | Days Averaged: {len(avg_df)}")
# #
# # # --- Main Execution ---
# # if __name__ == "__main__":
# #     print("📡 Fetching PM2.5 data from Urban Observatory...")
# #     pm25_data = fetch_pm25_data()
# #
# #     if not pm25_data:
# #         print("⚠️ No PM2.5 data found.")
# #     else:
# #         df_raw = pd.DataFrame(pm25_data)
# #         df_avg = compute_daily_average(df_raw)
# #         save_to_excel(df_raw, df_avg, OUTPUT_FILE)
#
#
# import pandas as pd
# import json
# import time
# import paho.mqtt.client as mqtt
#
# # --- Configuration ---
# EXCEL_FILE = "pm25_data.xlsx"
# MQTT_BROKER = "localhost"   # Change if needed
# MQTT_PORT = 1883
# TOPIC = "pm25/data"         # Keep consistent with your receiver
#
# # --- MQTT Function ---
# def send_to_mqtt(data, topic):
#     client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
#     client.connect(MQTT_BROKER, MQTT_PORT, 60)
#
#     for record in data:
#         # Convert Timestamp to string and rename keys
#         payload = {
#             "timestamp": str(record.get("Date")),
#             "pollutant": "PM2.5",
#             "value": record.get("Daily_Avg_PM2.5")
#         }
#         payload_json = json.dumps(payload)
#         client.publish(topic, payload_json)
#         print(f"✅ Sent to {topic}: {payload_json}")
#         time.sleep(0.5)
#
#     client.disconnect()
#
# # --- Read Excel (Only Daily Average Sheet) ---
# def read_daily_avg(filename):
#     print(f"📖 Reading daily average data from: {filename}")
#     df = pd.read_excel(filename, sheet_name="Daily_Average_PM2.5")
#     return df
#
# # --- Convert DataFrame to Records ---
# def df_to_records(df):
#     return df.to_dict(orient="records")
#
# # --- Main Execution ---
# if __name__ == "__main__":
#     df_avg = read_daily_avg(EXCEL_FILE)
#     avg_records = df_to_records(df_avg)
#
#     print(f"📊 Found {len(avg_records)} daily average records.")
#     send_to_mqtt(avg_records, TOPIC)
#     print("✅ All daily average PM2.5 data sent successfully!")


import requests
import pandas as pd
import json
import time
import paho.mqtt.client as mqtt
from datetime import datetime

# --- Configuration ---
API_URL = "https://raw.githubusercontent.com/ncl-iot-team/CSC8112/main/data/uo_data.min.json"
EXCEL_FILE = "pm25_data.xlsx"
MQTT_BROKER = "localhost"   # Change to your broker IP if remote
MQTT_PORT = 1883
TOPIC = "pm25/data"


# --- Step 1: Fetch PM2.5 Data ---
def fetch_pm25_data():
    print("📡 Fetching PM2.5 data from Urban Observatory...")
    response = requests.get(API_URL)
    response.raise_for_status()
    data = response.json()

    pm25_records = []
    for sensor in data.get("sensors", []):
        sensor_data = sensor.get("data")
        if not isinstance(sensor_data, dict):
            continue
        if "PM2.5" in sensor_data:
            for entry in sensor_data["PM2.5"]:
                ts = entry.get("Timestamp")
                val = entry.get("Value")
                if ts and val is not None:
                    ts_dt = datetime.utcfromtimestamp(ts / 1000)
                    pm25_records.append({
                        "Timestamp": ts_dt,
                        "Date": ts_dt.date(),
                        "Pollutant": "PM2.5",
                        "Value": val
                    })
    print(f"✅ Collected {len(pm25_records)} PM2.5 readings.")
    return pm25_records


# --- Step 2: Compute Daily Average ---
def compute_daily_average(df):
    daily_avg = df.groupby("Date")["Value"].mean().reset_index()
    daily_avg.rename(columns={"Value": "Daily_Avg_PM2.5"}, inplace=True)
    print(f"📊 Computed daily averages for {len(daily_avg)} days.")
    return daily_avg


# --- Step 3: Save to Excel ---
def save_to_excel(raw_df, avg_df, filename):
    with pd.ExcelWriter(filename, engine="openpyxl") as writer:
        raw_df.to_excel(writer, index=False, sheet_name="Raw_PM2.5_Data")
        avg_df.to_excel(writer, index=False, sheet_name="Daily_Average_PM2.5")
    print(f"💾 Data saved to '{filename}' successfully!")


# --- Step 4: Read Daily Average Sheet ---
def read_daily_avg(filename):
    print(f"📖 Reading daily average data from: {filename}")
    df = pd.read_excel(filename, sheet_name="Daily_Average_PM2.5")
    return df


# --- Step 5: Send Data via MQTT ---
def send_to_mqtt(data, topic):
    print(f"🚀 Sending data to MQTT broker at '{MQTT_BROKER}:{MQTT_PORT}' on topic '{topic}'...")
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.connect(MQTT_BROKER, MQTT_PORT, 60)

    for record in data:
        payload = {
            "timestamp": str(record.get("Date")),
            "pollutant": "PM2.5",
            "value": record.get("Daily_Avg_PM2.5")
        }
        payload_json = json.dumps(payload)
        client.publish(topic, payload_json)
        print(f"✅ Sent: {payload_json}")
        time.sleep(0.5)

    client.disconnect()
    print("🏁 All data sent successfully!")


# --- Main Execution ---
if __name__ == "__main__":
    # Fetch and process data
    pm25_data = fetch_pm25_data()
    if not pm25_data:
        print("⚠️ No PM2.5 data found.")
        exit()

    df_raw = pd.DataFrame(pm25_data)
    df_avg = compute_daily_average(df_raw)
    save_to_excel(df_raw, df_avg, EXCEL_FILE)

    # Read again and send over MQTT
    df_avg_reload = read_daily_avg(EXCEL_FILE)
    avg_records = df_avg_reload.to_dict(orient="records")
    send_to_mqtt(avg_records, TOPIC)
