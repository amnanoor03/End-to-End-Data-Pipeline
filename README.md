# End-to-End ETL Pipeline: Open Weather Analytics

## 🎯 Project Objective
This project demonstrates a production-ready ETL (Extract, Transform, Load) pipeline that ingests real-time weather data for multiple global cities, processes it for analytical readiness, and stores it in an optimized columnar format (Parquet).

## 🏗️ Data Architecture
The pipeline follows a modular architecture designed for scalability:
1. **Extraction:** Automated REST API consumption from OpenWeatherMap with robust error handling and logging.
2. **Transformation:** Data cleaning and schema flattening using Python (Pandas). Features include temperature unit conversion and metadata injection (timestamps).
3. **Storage (Load):** Data is converted to **Apache Parquet** with **Snappy compression**. This format is specifically chosen for its high performance in OLAP environments like **BigQuery** and **AWS Athena**.

## 🛠️ Tech Stack
* **Language:** Python 3.x
* **Libraries:** Pandas (Transformation), Requests (API), PyArrow (Columnar Storage), Python-Dotenv (Security)
* **Storage Format:** Parquet (Columnar)
* **DevOps:** Git, Logging, Environment Variable Management (.env)

## 📊 Analytical SQL (BI Reporting)
The resulting dataset is optimized for queries such as:
* **Trend Analysis:** Monitoring temperature fluctuations across different timeframes.
* **Aggregations:** Calculating average humidity and peak wind speeds per region.
*(See `analytical_queries.sql` for implementation)*

## 🚀 How to Run
1. Clone the repository.
2. Create a `.env` file and add your `WEATHER_API_KEY`.
3. Run `pip install -r requirements.txt`.
4. Execute `python etl_pipeline.py`.