import pandas as pd
import requests
import logging
from datetime import datetime
import os
from dotenv import load_dotenv

# Load variables from .env file
load_dotenv()
MY_API_KEY = os.getenv("WEATHER_API_KEY")

# 1. Setup Logging (Shows you know how to monitor pipelines)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def extract_weather_data(cities, api_key):
    """Fetches raw JSON data from OpenWeather API for multiple cities."""
    raw_data_list = []
    for city in cities:
        try:
            logging.info(f"Fetching data for {city}...")
            url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
            response = requests.get(url)
            response.raise_for_status()  # Check for HTTP errors
            raw_data_list.append(response.json())
        except Exception as e:
            logging.error(f"Failed to fetch data for {city}: {e}")
    return raw_data_list


def transform_data(raw_json_list):
    """Cleans, flattens, and prepares data for analytical use."""
    logging.info("Starting data transformation...")
    processed_records = []

    for item in raw_json_list:
        record = {
            "city_name": item.get("name"),
            "country": item.get("sys", {}).get("country"),
            "temperature_c": item.get("main", {}).get("temp"),
            "humidity": item.get("main", {}).get("humidity"),
            "weather_condition": item["weather"][0]["description"],
            "wind_speed": item.get("wind", {}).get("speed"),
            "extracted_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        processed_records.append(record)

    return pd.DataFrame(processed_records)


def load_data(df, file_format="parquet"):
    """Saves the final dataframe to an optimized storage format."""
    file_name = f"weather_analytics_data.{file_format}"
    try:
        if file_format == "parquet":
            # Using snappy compression (standard for BigQuery/Cloud)
            df.to_parquet(file_name, engine='pyarrow', compression='snappy', index=False)
        else:
            df.to_csv(file_name, index=False)
        logging.info(f"Data successfully loaded to {file_name}")
    except Exception as e:
        logging.error(f"Error loading data: {e}")


if __name__ == "__main__":
    # CONFIGURATION
    # DON'T re-define MY_API_KEY here. It is already loaded at the top!
    CITIES_TO_TRACK = ["Lahore", "Karachi", "Islamabad", "Dubai", "London"]

    # RUN THE PIPELINE
    raw_data = extract_weather_data(CITIES_TO_TRACK, MY_API_KEY)
    if raw_data:
        clean_df = transform_data(raw_data)
        load_data(clean_df, file_format="parquet")
        print(clean_df.head())