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

# Load last processed file (if exists)

if os.path.exists(control_file_path):
    try:
        with open(control_file_path, "r") as f: 
            content = f.read().strip() 
            if content: 
                control_data = json.load(f) 
                last_processed = control_data.get("LastProcessedFile", start_from) 
                print(f"📂 Last processed file: {last_processed}") 
            else: print("⚠️ No previous control file found — starting fresh.") 
            control_data = {} 
            last_processed = start_from
                    
    except json.JSONDecodeError:
        print("⚠️ Control file is corrupted or not valid JSON — resetting.")
        control_data = {}
        last_processed = start_from
else:
    print("⚠️ No previous control file found — starting fresh.")
    control_data = {}
    last_processed = start_from

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Fetch list of files from S3

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
print(f"Found {len(files_2025)} files for 2025.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Filter to files newer than or equal to last_processed

files_to_download = [f for f in files_2025 if f["Key"] >= last_processed]

if not files_to_download:
    print("All 2025 files already processed. Nothing new to download.")
else:
    print(f"{len(files_to_download)} new files to download starting from {last_processed}.")

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

        # Update the control file after each download
        metadata = {
            "LastProcessedFile": file_info["Key"],
            "FilePath": local_path,
            "Size": file_info["Size"],
            "LastModified": file_info["LastModified"]
        }
        os.makedirs(os.path.dirname(control_file_path), exist_ok=True)

        with open(control_file_path, "w") as control:
            json.dump(metadata, control, indent=2)

        print(f"🗂️ Control file synced to Lakehouse → {metadata['LastProcessedFile']}")

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
