# 🌦️ Weather Data ETL Pipeline

A modular and extensible ETL (Extract, Transform, Load) pipeline built in Python for collecting forecast and historical weather data for multiple cities using the [WeatherAPI](https://www.weatherapi.com/). The pipeline handles retries, deduplication, per-city data storage, and structured logging — making it ideal for data analysis, automation, or integration with data engineering workflows.

---

## 📌 Features

- 🔁 **ETL Pipeline**: Extracts weather data, transforms it into structured rows, and loads it into per-city CSV files.
- 🧠 **Smart Deduplication**: Skips inserting duplicate records based on date and data type (forecast/history).
- 🔄 **Retry Mechanism**: Automatically retries failed API calls with exponential backoff.
- 📁 **Per-City CSV Storage**: Organizes data cleanly inside a `weather_data/` folder.
- 🧪 **Robust Error Logging**: Catches and logs errors with traceback information.
- 🛠️ **Configurable**: Easily specify your cities and API key via `secrets.ini`.

---
