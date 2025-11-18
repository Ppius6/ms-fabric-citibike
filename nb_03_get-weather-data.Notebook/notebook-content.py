# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c84f89ea-9d33-4023-bd96-fb32e4017931",
# META       "default_lakehouse_name": "LH",
# META       "default_lakehouse_workspace_id": "778b8b43-34a7-4b55-a273-7763a295d386",
# META       "known_lakehouses": [
# META         {
# META           "id": "c84f89ea-9d33-4023-bd96-fb32e4017931"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Libraries
import requests
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime, timedelta
import json

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Initializing Spark
spark = SparkSession.builder.getOrCreate()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Parameters
start_date = start_date
end_date = end_date
latitude = latitude
longitude = longitude
max_trip_date = max_trip_date

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Add validation and type conversion
def validate_parameters():
    """Validate and convert parameters with fallbacks"""
    # Convert strings to appropriate types with fallbacks
    start_date_valid = str(start_date) if start_date else "2025-01-01"
    end_date_valid = str(end_date) if end_date else "2025-12-31"
    latitude_valid = float(latitude) if latitude else 40.728
    longitude_valid = float(longitude) if longitude else -74.077
    max_trip_date_valid = str(max_trip_date) if max_trip_date else "2025-12-31"
    
    print("🔧 Validated Parameters:")
    print(f"  start_date: {start_date_valid}")
    print(f"  end_date: {end_date_valid}")
    print(f"  latitude: {latitude_valid} (type: {type(latitude_valid)})")
    print(f"  longitude: {longitude_valid} (type: {type(longitude_valid)})")
    print(f"  max_trip_date: {max_trip_date_valid}")
    
    return start_date_valid, end_date_valid, latitude_valid, longitude_valid, max_trip_date_valid

# Validate parameters
start_date, end_date, latitude, longitude, max_trip_date = validate_parameters()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def setup_weather_control():
    """
    Create weather control table if it doesn't exist (for first run)
    """
    try:
        tables = spark.catalog.listTables()
        control_exists = any(table.name == "weather_control" for table in tables)
        
        if not control_exists:
            print("🆕 Creating weather_control table for first run...")
            
            # Create table using Spark SQL
            spark.sql("""
                CREATE TABLE weather_control (
                    last_processed_date DATE,
                    last_weather_date DATE,
                    latitude FLOAT,
                    longitude FLOAT,
                    updated_at TIMESTAMP
                )
            """)
            
            # Insert initial record
            spark.sql("""
                INSERT INTO weather_control VALUES (
                    CAST('2024-12-31' AS DATE),  -- last_processed_date
                    CAST('2024-12-31' AS DATE),  -- last_weather_date  
                    40.728,                      -- latitude
                    -74.077,                     -- longitude
                    CURRENT_TIMESTAMP()          -- updated_at
                )
            """)
            
            print("Created weather_control table with initial data")
        else:
            print("weather_control table already exists")
            
    except Exception as e:
        print(f"Error setting up control table: {e}")
        # Don't raise error - continue with existing table if it exists

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get the weather data

def get_weather_data(start_date, end_date, latitude, longitude):
    """
    Fetch historical weather data for specific coordinates
    """
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": "temperature_2m,relative_humidity_2m,precipitation,weather_code,wind_speed_10m",
        "timezone": "America/New_York"
    }

    try:
        print(f"Fetching weather for ({latitude}, {longitude}) from {start_date} to {end_date}")
        response = requests.get("https://archive-api.open-meteo.com/v1/archive", params=params)
        response.raise_for_status()
        data = response.json()

        hourly_data = data.get('hourly', {})
        df = pd.DataFrame({
            'datetime': hourly_data.get('time', []),
            'temperature_c': hourly_data.get('temperature_2m', []),
            'humidity_percent': hourly_data.get('relative_humidity_2m', []),
            'precipitation_mm': hourly_data.get('precipitation', []),
            'weather_code': hourly_data.get('weather_code', []),
            'wind_speed_kmh': hourly_data.get('wind_speed_10m', [])
        })

        df['datetime'] = pd.to_datetime(df['datetime'])
        print(f"Fetched {len(df)} weather records")
        return df
        
    except Exception as e:
        print(f"Error fetching weather data: {e}")
        return pd.DataFrame()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create the weather table

def update_weather_tables(weather_df):
    """
    Merge new weather data with existing data
    """
    if weather_df.empty:
        print("No weather data to load")
        return

    # 1. Append to weather_data table
    spark_df = spark.createDataFrame(weather_df)

    spark_df = spark_df.withColumn("date_key", 
        expr("CAST(REPLACE(SUBSTRING(CAST(datetime AS STRING), 1, 10), '-', '') AS INT)")
    )

    spark_df = spark_df.withColumn("time_key", hour(col("datetime")))

    spark_df = spark_df.withColumn("weather_condition",
        expr("""
            CASE 
                WHEN weather_code = 0 THEN 'Clear sky'
                WHEN weather_code IN (1, 2, 3) THEN 'Partly cloudy'
                WHEN weather_code IN (45, 48) THEN 'Fog'
                WHEN weather_code IN (51, 53, 55) THEN 'Drizzle'
                WHEN weather_code IN (61, 63, 65) THEN 'Rain'
                WHEN weather_code IN (71, 73, 75) THEN 'Snow'
                WHEN weather_code IN (95, 96, 99) THEN 'Thunderstorm'
                ELSE 'Unknown'
            END
        """)
        )


    # Add metadata columns
    spark_df = spark_df.withColumn("loaded_at", current_timestamp())

    # Append to weather_data table
    spark_df.write.mode("append").format("delta").saveAsTable("weather_data")

    print(f"Appended {spark_df.count()} records to weather records.")

    if isinstance(max_trip_date, str):
        trip_date = datetime.strptime(max_trip_date, '%Y-%m-%d').date()
    else:
        trip_date = max_trip_date

    # 2. Update control table
    control_data = [(
        trip_date,              # last_processed_date
        trip_date,              # last_weather_date (now caught up)
        float(latitude),        # latitude
        float(longitude),       # longitude
        datetime.now()          # updated_at
    )]
    
    control_schema = StructType([
        StructField("last_processed_date", DateType(), True),
        StructField("last_weather_date", DateType(), True),
        StructField("latitude", FloatType(), True),
        StructField("longitude", FloatType(), True),
        StructField("updated_at", TimestampType(), True)
    ])

    control_df = spark.createDataFrame(control_data, control_schema)
    control_df.write.mode("overwrite").format("delta").saveAsTable("weather_control")

    print(f"Updated weather_control to date: {max_trip_date}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Ensure control table exists (for first run)
setup_weather_control()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"Date range of weather data fetch: {start_date} to {end_date}")
print(f"Coordinates: ({latitude}, {longitude})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Fetch weather data
weather_df = get_weather_data(start_date, end_date, latitude, longitude)

# Update tables if we have data
if not weather_df.empty:
    update_weather_tables(weather_df)

    # Show summary
    summary = spark.sql("""
        SELECT 
            COUNT(*) as total_records,
            MIN(datetime) as earliest_date,
            MAX(datetime) as latest_date
        FROM weather_data
    """).collect()[0]
    
    print(f"Final weather data summary:")
    print(f"Total records: {summary['total_records']:,}")
    print(f"Date range: {summary['earliest_date']} to {summary['latest_date']}")
else:
    print("No weather data was fetched")

print("Weather data fetch completed!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
