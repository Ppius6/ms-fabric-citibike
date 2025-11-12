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
import xml.etree.ElementTree as ET
import requests
import os
from datetime import datetime
import json
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark.sql.functions import current_timestamp

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Parameters

# start_from = "JC-202501-citibike-tripdata.csv.zip" # Useful for initial run
# s3_base_url = "https://s3.amazonaws.com/tripdata"
# control_file_path = "/lakehouse/default/Files/control/last_processed.json"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Initialize Spark Session
spark = SparkSession.builder.getOrCreate()

def get_last_processed_from_table():
    """Read last processed file from Lakehouse table"""
    try:
        # Check if the control table exists
        tables = spark.catalog.listTables()
        table_exists = any(table.name == "download_control" for table in tables)

        if table_exists:
            control_df = spark.sql(
                "SELECT TOP 1 LastProcessedFile FROM download_control ORDER BY ProcessingTimestamp DESC"
            )

            if control_df.count() > 0:
                last_processed = control_df.collect()[0]["LastProcessedFile"]
                print(f"📂 Last processed file from table: {last_processed}")
                return last_processed
    except Exception as e:
        print(f"⚠️ Error reading from control table: {e}")
    
    print("⚠️ No control table found or error reading — using default start_from")
    return start_from

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def update_control_table(file_info, local_path):
    """Update control table with latest processed file"""
    try:
        # Create control data
        control_data = [(
            file_info["Key"],           # LastProcessedFile
            local_path,                 # FilePath
            file_info["Size"],          # Size
            file_info["LastModified"],  # LastModified (from S3)
            datetime.now().isoformat()  # ProcessingTimestamp
        )]

        # Schema
        schema = StructType([
            StructField("LastProcessedFile", StringType(), True),
            StructField("FilePath", StringType(), True),
            StructField("Size", LongType(), True),
            StructField("LastModified", StringType(), True),
            StructField("ProcessingTimestamp", StringType(), True)
        ])

        # Create the DataFrame and write to table
        df = spark.createDataFrame(control_data, schema)
        df.write.mode("overwrite").format("delta").saveAsTable("download_control")

        print(f"🗂️ Control table updated → {file_info['Key']}")

        # Verify the write
        verify_df = spark.sql("SELECT LastProcessedFile FROM download_control")
        latest_file = verify_df.collect()[0]["LastProcessedFile"]
        print(f" Verification: Latest in table = {latest_file}")
        
    except Exception as e:
        print(f" Error updating control table: {e}")
        raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load last processed file from Lakehouse table

last_processed = get_last_processed_from_table()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Fetch list of files from S3

print("🌐 Fetching file list from S3...")
response = requests.get(f"{s3_base_url}?list-type=2")
response.raise_for_status()

ns = {"ns": "http://s3.amazonaws.com/doc/2006-03-01/"}
root = ET.fromstring(response.text)

files = []

for item in root.findall("ns:Contents", ns):
    key = item.find("ns:Key", ns).text
    last_modified = item.find("ns:LastModified", ns).text
    size = int(item.find("ns:Size", ns).text)
    files.append({"Key": key, "LastModified": last_modified, "Size": size})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Download data beginning from 2025

files_2025 = [
    f for f in files if f['Key'].startswith("JC-2025")
]

if not files_2025:
    raise ValueError("No 2025 files found!")

files_2025.sort(key=lambda x: x["Key"])
print(f"📁 Found {len(files_2025)} files for 2025.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Filter to files newer than or equal to last_processed

files_to_download = [f for f in files_2025 if f["Key"] >= last_processed]

if not files_to_download:
    # Show current state for pipeline
    latest = {"Key": last_processed, "Status": "No new files"}
else:
    print(f"⬇️ {len(files_to_download)} new files to download starting from {last_processed}.")

    for file_info in files_to_download:
        file_url = f"{s3_base_url}/{file_info['Key']}"
        local_path = f"/lakehouse/default/Files/{file_info['Key']}"
        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        print(f"⬇️ Downloading: {file_url}")
        r = requests.get(file_url)
        r.raise_for_status()

        with open(local_path, "wb") as out_file:
            out_file.write(r.content)

        print(f"✅ Saved file: {local_path}")

        # Update the control table after each successful download
        update_control_table(file_info, local_path)

latest = files_to_download[-1] if files_to_download else {"Key": last_processed}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("📤 Output metadata for pipeline:")
print(json.dumps(latest, indent=2))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Final verification
print("\n🔍 Final control table status:")
try:
    final_check = spark.sql("SELECT LastProcessedFile, ProcessingTimestamp FROM download_control")
    final_check.show()
except Exception as e:
    print(f"Note: Could not display final table status: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
