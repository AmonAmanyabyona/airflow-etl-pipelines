# Airflow ETL Project: Weather & Berlin Cafés

This project demonstrates how to build and run ETL pipelines with **Apache Airflow** and **Postgres** using the Astro CLI.  
I created two DAGs:
- **Weather ETL Pipeline**: Fetches current weather data from the Open-Meteo API and stores it in Postgres.
- **Berlin Cafés Scraper**: Queries OpenStreetMap’s Overpass API for cafés in Berlin and stores them in Postgres.

---

## 🚀 Project Setup

### Requirements
- Docker & Docker Compose
- Astro CLI (for Airflow)
- Python packages:
  ```bash
  pip install overpy 
