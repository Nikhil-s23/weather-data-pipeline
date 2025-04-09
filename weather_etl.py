import csv
import logging
import requests
import sys
import traceback
import datetime
from configparser import ConfigParser
from urllib import parse
from requests.exceptions import ConnectTimeout, HTTPError, ReadTimeout, Timeout, ConnectionError
import time
from pathlib import Path

BASE_WEATHER_API_URL = 'http://api.weatherapi.com/v1/'
DATA_DIR = Path("weather_data")
DATA_DIR.mkdir(exist_ok=True)


class WeatherExtractor:
    def __init__(self):
        config = ConfigParser()
        config.read("secrets.ini")
        self.api_key = config["weather"]["api_key"]
        self.cities = [city.strip() for city in config["weather"]["cities"].split(',')]
        self.header = ['Date', 'Location', 'Country', 'Min_Temp', 'Max_Temp', 'Humidity', 'Air_Quality', 'Type']
        self.city_records = {}
        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    def retry(no_of_retries=3, initial_delay=5, backoff=True):
        def decorator(func):
            def wrapper(*args, **kwargs):
                delay = initial_delay
                for attempt in range(1, no_of_retries + 1):
                    try:
                        return func(*args, **kwargs)
                    except (ConnectTimeout, HTTPError, ReadTimeout, Timeout, ConnectionError) as e:
                        logging.warning(
                            f"[Retry {attempt}/{no_of_retries}] {func.__name__} failed with: {e}. Retrying in {delay} seconds..."
                        )
                        time.sleep(delay)
                        if backoff:
                            delay *= 2
                logging.error(f"All {no_of_retries} retries failed for {func.__name__}")
                return None
            return wrapper
        return decorator

    def exception_handling(self):
        ex_type, ex_value, ex_traceback = sys.exc_info()
        trace_back = traceback.extract_tb(ex_traceback)
        stack_trace = [f"File : {trace[0]}, Line : {trace[1]}, Func.Name : {trace[2]}, Message : {trace[3]}" for trace in trace_back]
        logging.error(f"Exception type : {ex_type.__name__}")
        logging.error(f"Exception message : {ex_value}")
        logging.error(f"Stack trace : {stack_trace}")

    def create_csv_for_city(self, city):
        file_path = DATA_DIR / f"{city.replace(' ', '_')}.csv"
        if not file_path.exists() or file_path.stat().st_size == 0:
            with open(file_path, 'w', newline='') as file:
                writer = csv.DictWriter(file, fieldnames=self.header)
                writer.writeheader()
                logging.info(f"CSV header written for {city}")
        return file_path

    @retry(no_of_retries=3, initial_delay=2, backoff=True)
    def fetch_data(self, url):
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logging.error(f"Failed to fetch data: {e}")
            self.exception_handling()
            return None

    def extract_and_write(self, data, mode):
        try:
            city = data['location']['name'].replace(' ', '_')
            file_path = self.create_csv_for_city(city)

            if city not in self.city_records:
                self.city_records[city] = set()
                if file_path.exists():
                    with open(file_path, 'r', newline='') as f:
                        reader = csv.DictReader(f)
                        for row in reader:
                            key = (row.get('Date'), row.get('Type'))
                            if all(key):
                                self.city_records[city].add(key)

            forecast_days = data['forecast']['forecastday']
            for day_data in forecast_days:
                day = day_data['day']
                row = {
                    'Date': day_data['date'],
                    'Location': data['location']['name'],
                    'Country': data['location']['country'],
                    'Min_Temp': day['mintemp_c'],
                    'Max_Temp': day['maxtemp_c'],
                    'Humidity': day['avghumidity'],
                    'Air_Quality': data.get('current', {}).get('air_quality', {}).get('co', 'N/A'),
                    'Type': mode.upper()
                }

                key = (row['Date'], row['Type'])
                if key not in self.city_records[city]:
                    with open(file_path, 'a', newline='') as file:
                        writer = csv.DictWriter(file, fieldnames=self.header)
                        writer.writerow(row)
                        logging.info(f"[{mode.upper()}] Wrote {city} on {row['Date']}")
                    self.city_records[city].add(key)
                else:
                    logging.info(f"Skipped duplicate for {city} on {row['Date']} ({mode.upper()})")
        except Exception as e:
            logging.error(f"Error extracting/writing {mode} data: {e}")
            self.exception_handling()

    def build_forecast_url(self, city):
        params = {
            "q": city,
            "days": 3,
            "aqi": "yes",
            "alerts": "no"
        }
        return f"{BASE_WEATHER_API_URL}forecast.json?key={self.api_key}&{parse.urlencode(params)}"

    def build_history_url(self, city, date_str):
        params = {
            "q": city,
            "dt": date_str
        }
        return f"{BASE_WEATHER_API_URL}history.json?key={self.api_key}&{parse.urlencode(params)}"

    def run_forecast_etl(self):
        for city in self.cities:
            url = self.build_forecast_url(city)
            logging.info(f"Fetching forecast for {city}")
            data = self.fetch_data(url)
            if data:
                self.extract_and_write(data, mode="forecast")

    def run_history_etl(self):
        today = datetime.date.today()
        for i in range(1, 4):
            history_date = (today - datetime.timedelta(days=i)).strftime('%Y-%m-%d')
            for city in self.cities:
                url = self.build_history_url(city, history_date)
                logging.info(f"Fetching history for {city} on {history_date}")
                data = self.fetch_data(url)
                if data:
                    self.extract_and_write(data, mode="history")

    def main(self):
        self.run_forecast_etl()
        self.run_history_etl()


if __name__ == "__main__":
    WeatherExtractor().main()
