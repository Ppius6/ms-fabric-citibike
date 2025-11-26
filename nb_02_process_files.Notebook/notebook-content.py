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

import zipfile
import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime
import glob
import shutil

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark = SparkSession.builder.getOrCreate()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# Parameters
source_folder = "/lakehouse/default/Files/"
target_table = "citibike_trips"
file_pattern = "JC-*-citibike-tripdata*.zip"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def extract_zip_files():
    """Extract zip files to Lakehouse Files area and return Spark-readable paths"""
    # Use a broader pattern to catch all variations
    zip_files = glob.glob(os.path.join(source_folder, "JC-2025*.zip"))
    
    print(f"Found {len(zip_files)} zip files to extract:")
    for zf in zip_files:
        print(f"   - {os.path.basename(zf)}")
    
    extracted_csv_paths = []
    
    for zip_path in zip_files:
        file_name = os.path.basename(zip_path)
        print(f"\nExtracting: {file_name}")
        
        try:
            # Create extraction directory in Lakehouse Files
            extract_dir_name = file_name.replace(".zip", "_extracted")
            extract_dir_path = os.path.join(source_folder, extract_dir_name)
            os.makedirs(extract_dir_path, exist_ok=True)
            
            # Extract zip file
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(extract_dir_path)
                print(f"Extracted to: {extract_dir_path}")
                
                # List extracted files
                extracted_files = os.listdir(extract_dir_path)
                print(f"Files extracted: {extracted_files}")
                
                # Find CSV files and create Spark-readable paths
                for extracted_file in extracted_files:
                    if extracted_file.endswith('.csv'):
                        # Full local path for file operations
                        local_csv_path = os.path.join(extract_dir_path, extracted_file)
                        
                        # Spark-readable path (relative to Lakehouse root)
                        spark_csv_path = f"Files/{extract_dir_name}/{extracted_file}"
                        
                        extracted_csv_paths.append({
                            'local_path': local_csv_path,
                            'spark_path': spark_csv_path,
                            'source_zip': file_name,
                            'extract_dir': extract_dir_path
                        })
                        
                        print(f"📄 Found CSV: {extracted_file}")
                        print(f"   Local path: {local_csv_path}")
                        print(f"   Spark path: {spark_csv_path}")
                    else:
                        print(f"Skipping non-CSV file: {extracted_file}")
            
            # Check if no CSV files were found
            if not any(f.endswith('.csv') for f in extracted_files):
                print(f"No CSV files found in {file_name}")
                        
        except Exception as e:
            print(f"Error extracting {file_name}: {e}")
    
    return extracted_csv_paths, zip_files

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def load_csv_files(extracted_csv_paths):
    """Load all extracted CSV files into a single DataFrame"""
    if not extracted_csv_paths:
        print("No CSV files to load")
        return None
    
    all_data = None
    
    for csv_info in extracted_csv_paths:
        spark_path = csv_info['spark_path']
        source_zip = csv_info['source_zip']
        
        print(f"\n Loading: {spark_path}")
        
        try:
            # Read CSV using Spark path
            df = spark.read.option("header", "true").option("inferSchema", "true").csv(spark_path)
            
            row_count = df.count()
            print(f"✅ Loaded {row_count} rows")
            print("📊 Schema:")
            df.printSchema()
            
            # Add metadata columns
            df = (df
                  .withColumn("source_zip_file", lit(source_zip))
                  .withColumn("processed_at", current_timestamp())
                  .withColumn("processing_batch_id", lit(datetime.now().strftime("%Y%m%d_%H%M%S")))
            )
            
            # Union with existing data
            if all_data is None:
                all_data = df
                print("Created initial DataFrame")
            else:
                all_data = all_data.union(df)
                print("Appended to existing DataFrame")
                
        except Exception as e:
            print(f"Error loading {spark_path}: {e}")
    
    return all_data

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def cleanup_files(zip_files, extracted_csv_paths):
    """Clean up zip files and extraction directories"""
    print(f"\nStarting cleanup...")
    
    # Clean up zip files
    if zip_files:
        print(f"Deleting {len(zip_files)} zip files...")
        for zip_path in zip_files:
            try:
                os.remove(zip_path)
                print(f"Deleted: {os.path.basename(zip_path)}")
            except Exception as e:
                print(f"Could not delete {zip_path}: {e}")
    
    # Clean up extraction directories
    if extracted_csv_paths:
        # Get unique extraction directories
        extract_dirs = set(csv_info['extract_dir'] for csv_info in extracted_csv_paths)
        print(f"Deleting {len(extract_dirs)} extraction directories...")
        
        for extract_dir in extract_dirs:
            try:
                shutil.rmtree(extract_dir, ignore_errors=True)
                print(f"Deleted: {os.path.basename(extract_dir)}")
            except Exception as e:
                print(f"Could not delete {extract_dir}: {e}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def update_processing_history(zip_files, success_count):
    """Update processing history table"""
    try:
        history_data = []
        for zip_path in zip_files:
            file_name = os.path.basename(zip_path)
            status = "processed" if success_count > 0 else "failed"
            history_data.append((
                file_name,
                status,
                datetime.now().isoformat(),
                ""
            ))
        
        schema = StructType([
            StructField("FileName", StringType(), True),
            StructField("Status", StringType(), True),
            StructField("ProcessedTimestamp", StringType(), True),
            StructField("Error", StringType(), True)
        ])
        
        df = spark.createDataFrame(history_data, schema)
        df.write.mode("append").format("delta").option("mergeSchema", "true").saveAsTable("file_processing_history")
        print(f"📝 Updated processing history for {len(zip_files)} files")
        
    except Exception as e:
        print(f"⚠️ Could not update processing history: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def initialize_processing_history():
    """Create processing history table if it doesn't exist"""
    try:
        tables = spark.catalog.listTables()
        table_exists = any(table.name == "file_processing_history" for table in tables)

        if not table_exists:
            schema = StructType([
                StructField("FileName", StringType(), True),
                StructField("Status", StringType(), True),
                StructField("ProcessedTimestamp", StringType(), True),
                StructField("Error", StringType(), True)
            ])

            # Create empty table
            empty_df = spark.createDataFrame([], schema)
            empty_df.write.format("delta").saveAsTable("file_processing_history")
            print("✅ Created file_processing_history table")

    except Exception as e:
        print(f"⚠️ Error initializing processing history: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def write_to_table(df):
    """Write DataFrame to target table"""
    if df is not None and df.count() > 0:
        try:
            total_rows = df.count()
            print(f" Writing {total_rows} rows to table: {target_table}")
            
            # Check if table exists
            tables = spark.catalog.listTables()
            table_exists = any(table.name == target_table for table in tables)

            if table_exists:
                # Append new data
                df.write.mode("append").format("delta").saveAsTable(target_table)
            else:
                df.write.mode("overwrite").format("delta").saveAsTable(target_table)
            
            # Verify write
            table_count = spark.sql(f"SELECT COUNT(*) as count FROM {target_table}").collect()[0]["count"]
            print(f" Successfully wrote {table_count} rows to {target_table}")

            return total_rows
            
        except Exception as e:
            print(f"Error writing to table: {e}")
            raise
    else:
        print("No data to write")
        return 0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def create_metadata_table():
    """Create metadata table to track max dates and ride IDs for each run"""
    try:
        tables = spark.catalog.listTables()
        metadata_exists = any(table.name == "citibike_metadata" for table in tables)

        if not metadata_exists:
            schema = StructType([
                StructField("run_id", StringType(), True),
                StructField("run_timestamp", TimestampType(), True),
                StructField("max_started_at", TimestampType(), True),
                StructField("max_ended_at", TimestampType(), True),
                StructField("latest_ride_id_start", StringType(), True),
                StructField("latest_ride_id_end", StringType(), True),
                StructField("total_records_processed", LongType(), True),
                StructField("files_processed", StringType(), True),
                StructField("processing_batch_id", StringType(), True)
            ])

            # Create empty table
            empty_df = spark.createDataFrame([], schema)
            empty_df.write.format("delta").saveAsTable("citibike_metadata")
            print("Created citibike_metadata table")
        else:
            print("citibike_metadata table already exists")
            
    except Exception as e:
        print(f"⚠️ Error creating metadata table: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def update_metadata_table(processing_batch_id, zip_files):
    """Update metadata table with current run information"""
    try:
        # Get the latest ride with max started_at and max ended_at
        max_dates_df = spark.sql("""
            SELECT 
                MAX(started_at) as max_started_at,
                MAX(ended_at) as max_ended_at,
                COUNT(*) as total_records
            FROM citibike_trips
        """)
        
        max_dates = max_dates_df.collect()[0]
        max_started_at = max_dates['max_started_at']
        max_ended_at = max_dates['max_ended_at']
        total_records = max_dates['total_records']
        
        # Get the ride ID for the latest started_at
        latest_start_ride_df = spark.sql("""
            SELECT ride_id 
            FROM citibike_trips 
            WHERE started_at = (SELECT MAX(started_at) FROM citibike_trips)
            ORDER BY processed_at DESC 
            LIMIT 1
        """)
        
        latest_start_ride_id = latest_start_ride_df.collect()[0]['ride_id'] if latest_start_ride_df.count() > 0 else "Unknown"
        
        # Get the ride ID for the latest ended_at
        latest_end_ride_df = spark.sql("""
            SELECT ride_id 
            FROM citibike_trips 
            WHERE ended_at = (SELECT MAX(ended_at) FROM citibike_trips)
            ORDER BY processed_at DESC 
            LIMIT 1
        """)
        
        latest_end_ride_id = latest_end_ride_df.collect()[0]['ride_id'] if latest_end_ride_df.count() > 0 else "Unknown"
        
        # Prepare metadata for current run
        metadata_data = [(
            f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}",  # run_id
            datetime.now(),                                      # run_timestamp
            max_started_at,                                      # max_started_at
            max_ended_at,                                        # max_ended_at
            latest_start_ride_id,                                # latest_ride_id_start
            latest_end_ride_id,                                  # latest_ride_id_end
            total_records,                                       # total_records_processed
            ", ".join([os.path.basename(zf) for zf in zip_files]),  # files_processed
            processing_batch_id                                  # processing_batch_id
        )]
        
        schema = StructType([
            StructField("run_id", StringType(), True),
            StructField("run_timestamp", TimestampType(), True),
            StructField("max_started_at", TimestampType(), True),
            StructField("max_ended_at", TimestampType(), True),
            StructField("latest_ride_id_start", StringType(), True),
            StructField("latest_ride_id_end", StringType(), True),
            StructField("total_records_processed", LongType(), True),
            StructField("files_processed", StringType(), True),
            StructField("processing_batch_id", StringType(), True)
        ])
        
        metadata_df = spark.createDataFrame(metadata_data, schema)
        metadata_df.write.mode("append").format("delta").saveAsTable("citibike_metadata")
        
        print(f"✅ Updated citibike_metadata table")
        print(f"   Max started_at: {max_started_at} (Ride: {latest_start_ride_id})")
        print(f"   Max ended_at: {max_ended_at} (Ride: {latest_end_ride_id})")
        print(f"   Total records: {total_records:,}")
        
    except Exception as e:
        print(f"⚠️ Error updating metadata table: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# Step 1: Initialize processing history table
print("Initializing processing history...")
initialize_processing_history()

# Step 1.5: Initialize metadata table
print("Initializing metadata table...")
create_metadata_table()

# Step 2: Extract, process, and clean up files
print("Extracting and processing files...")
zip_files = glob.glob(os.path.join(source_folder, file_pattern))
extracted_csv_paths, zip_files = extract_zip_files()

# Step 3: Load CSV files
print("\n📖 Step 3: Loading CSV files...")
final_data = load_csv_files(extracted_csv_paths)

# Step 4: Write to table
print("\nStep 4: Writing to target table...")
success_count = write_to_table(final_data)

# Step 4.5: Update metadata table with current run info
if success_count > 0:
    print("\nStep 4.5: Updating metadata table...")
    processing_batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    update_metadata_table(processing_batch_id, zip_files)

# Step 5: Update processing history
print("\nStep 5: Updating processing history...")
update_processing_history(zip_files, success_count)

# Step 6: Cleanup
print("\nStep 6: Cleaning up files...")
cleanup_files(zip_files, extracted_csv_paths)

print("\n✅ Pipeline completed successfully!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
