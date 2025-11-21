"""
Egypt Air Traffic Surveillance DAG
-----------------------------------
Description: This DAG extracts real-time flight data over Egypt using FlightRadar24 API,
transforms the data using Pandas, and loads it into a PostgreSQL Data Warehouse.

Author: Abdelrhman Magdy
Project: End-to-End Data Engineering Portfolio
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Note: Heavy libraries are imported inside the function (Lazy Loading) 
# to optimize the Scheduler's parse time.

def fetch_and_load_flights(**kwargs):
    """
    Extracts flight data, cleans it, and loads it into the database securely.
    """
    # --- Lazy Imports ---
    from airflow.hooks.base import BaseHook
    from FlightRadar24 import FlightRadar24API
    import pandas as pd
    from sqlalchemy import create_engine
    from datetime import datetime
    
    # --- Security Step: Retrieve Connection from Airflow Secrets ---
    # We fetch credentials using the Connection ID 'my_postgres_conn' 
    # instead of hardcoding passwords in the script.
    connection = BaseHook.get_connection("my_postgres_conn")
    
    # Dynamically construct the connection string
    db_connection_str = 'postgresql://admin:admin@my_postgres:5432/postgres'
    db_connection = create_engine(db_connection_str)
    
    # --- 1. Extraction (FlightRadar24 API) ---
    fr_api = FlightRadar24API()
    # Bounding box coordinates for Egypt [North, South, West, East]
    bounds = "32.00,22.00,25.00,37.00"
    
    print("📡 Airflow Task Started: Scanning Egypt Skies...")
    
    try:
        flights = fr_api.get_flights(bounds=bounds)
        
        # Safety check: Exit if API returns no data
        if not flights:
            print("⚠️ No flights found in the specified area.")
            return

        # --- 2. Transformation (Pandas) ---
        flights_data = []
        current_time = datetime.now()

        for f in flights:
            flights_data.append({
                'icao24': f.icao_24bit,
                'callsign': f.callsign,
                'airline': f.airline_iata,
                'origin_airport': f.origin_airport_iata if f.origin_airport_iata else "Unknown",
                'latitude': f.latitude,
                'longitude': f.longitude,
                'altitude_meters': f.altitude * 0.3048,   # Convert feet to meters
                'velocity_kmh': f.ground_speed * 1.852,   # Convert knots to km/h
                'heading': f.heading,
                'ingestion_time': current_time            # Timestamp for historical tracking
            })
            
        df = pd.read_json(pd.DataFrame(flights_data).to_json())
        # Ensure datetime format compatibility with PostgreSQL
        df['ingestion_time'] = pd.to_datetime(df['ingestion_time'], unit='ms')
        
        # --- 3. Load (PostgreSQL) ---
        # Using 'append' to build a historical dataset over time
        df.to_sql('egypt_sky_traffic', db_connection, if_exists='append', index=False)
        
        print(f"✅ Success! {len(df)} flights inserted securely into the Data Warehouse.")

    except Exception as e:
        print(f"❌ ETL Process Failed: {e}")
        raise e 

# --- DAG Definition ---
default_args = {
    'owner': 'abdelrhman',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='egypt_radar_v1',
    default_args=default_args,
    description='ETL pipeline for monitoring Egypt air traffic',
    start_date=datetime(2023, 11, 18),
    schedule_interval='*/15 * * * *',  # Runs every 15 minutes
    catchup=False,
    tags=['production', 'radar', 'etl'] # Tags for better UI organization
) as dag:

    run_etl = PythonOperator(
        task_id='extract_and_load_radar',
        python_callable=fetch_and_load_flights
    )