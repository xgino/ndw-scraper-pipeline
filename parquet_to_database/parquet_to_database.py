import os
import sqlite3
import pandas as pd
import numpy as np 
from datetime import datetime
from shapely.wkb import loads as wkb_loads
from tqdm import tqdm

from pathlib import Path
import logging

# Setup logger with an absolute path
log_file = Path(__file__).resolve().parent.parent / 'logs' / 'parquet_to_database.log'
log_file.parent.mkdir(parents=True, exist_ok=True)  # Ensure the logs directory exists

# Setup  logger
logging.basicConfig(
    filename=str(log_file),
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('parquet_to_database')


class PARQUET_TO_DATABASE:
    def __init__(self, data_folder="../convert_to_parquet/parquet", db_path="../NDW.db", process_date=None):
        self.data_folder = data_folder
        self.db_path = db_path
        # Format the date as 'DD-MM-YYYY'. If no date is passed, use today's date.
        self.process_date = process_date or datetime.today().strftime('%d-%m-%Y')  # Default to today's date

    def find_parquet_files(self):
        try:
            parquet_files = []
            # Log the directory being searched
            logger.info(f"Searching for parquet files in: {self.data_folder}")
            
            for root, _, files in os.walk(self.data_folder):
                for file in files:
                    if file.endswith(".parquet"):
                        file_path = os.path.join(root, file)
                        parquet_files.append(file_path)
                        # Log each found parquet file and its size
                        file_size = os.path.getsize(file_path) / (1024 * 1024)  # Size in MB
                        logger.info(f"Found file: {file_path}, Size: {file_size:.2f} MB")
            
            if not parquet_files:
                logger.warning("No parquet files found.")
                
            return parquet_files
        except Exception as e:
            logger.error(f"Error while finding parquet files: {e}")
            return []

    def preprocess_parquet(self, file_path):
        try:
            logger.info(f"Started processing file: {file_path}")

            # Read the Parquet file
            df = pd.read_parquet(file_path)
            logger.info(f"File {file_path} loaded with {len(df)} rows")

            # Convert NaN or empty columns to None
            df = df.where(pd.notnull(df), None)

            # Handle geometry column conversion
            if "geometry" in df.columns:
                logger.info(f"Processing geometry column in {file_path}")
                
                def process_geometry(geometry):
                    if isinstance(geometry, bytes):
                        geom_obj = wkb_loads(geometry)
                        if geom_obj.geom_type in ["Point", "LineString"]:
                            return geom_obj.wkt  # Convert to WKT for SQLite compatibility
                        return None
                    return None
                
                df["geometry"] = df["geometry"].apply(process_geometry)

            # Convert numpy.ndarray and dict columns to string
            for col in df.columns:
                df[col] = df[col].apply(lambda x: str(x) if isinstance(x, (np.ndarray, dict)) else x)

            # Remove empty or all-NA columns
            df = df.dropna(axis=1, how="all")

            # Add a 'scraped_date' column with today's date (or a custom date)
            df['scraped_date'] = self.process_date

            logger.info(f"Preprocessed file {file_path} with {len(df)} rows remaining")
            return df
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {e}")
            raise

    def insert_into_db(self, df, table_name):
        try:
            # Log the start of the insertion process
            logger.info(f"Started inserting data into {table_name} table in database {self.db_path}")
            
            with sqlite3.connect(self.db_path) as conn:
                # Check if the table exists
                table_exists_query = f"""
                SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';
                """
                table_exists = pd.read_sql(table_exists_query, conn).empty == False
                
                if table_exists:
                    # Log that the table exists and data is being appended
                    logger.info(f"Table {table_name} exists. Appending data.")
                    
                    # Append data to the existing table
                    existing_data = pd.read_sql(f"SELECT * FROM {table_name};", conn)
                    
                    # Drop empty or all-NA columns
                    df_cleaned = df.dropna(axis=1, how='all')
                    existing_data_cleaned = existing_data.dropna(axis=1, how='all')
                    
                    combined_data = pd.concat([existing_data_cleaned, df_cleaned])
                    combined_data.drop_duplicates(inplace=True)
                    combined_data.to_sql(table_name, conn, if_exists="replace", index=False)
                    
                    logger.info(f"Successfully appended data to {table_name}.")
                else:
                    # Log that the table does not exist and a new table is being created
                    logger.info(f"Table {table_name} does not exist. Creating new table.")
                    
                    # Create a new table
                    df.to_sql(table_name, conn, if_exists="replace", index=False)
                    
                    logger.info(f"Successfully created and inserted data into {table_name}.")
        except Exception as e:
            # Log error with detailed information
            logger.error(f"Error inserting data into {table_name} at {datetime.now()}. Error: {e}")
            logger.error(f"DataFrame columns: {df.columns.tolist()}")
            raise

    def process(self):
        try:
            # Check if database exists
            if not os.path.exists(self.db_path):
                print(f"Database not found. Creating a new database at {self.db_path}.")
                logger.info(f"Database not found. Creating a new database at {self.db_path}.")

            # Find parquet files
            parquet_files = self.find_parquet_files()
            print(f"Found {len(parquet_files)} parquet files.")
            logger.info(f"Found {len(parquet_files)} parquet files.")

            # Process each parquet file
            for file_path in tqdm(parquet_files, desc="Processing Parquet Files"):
                table_name = os.path.splitext(os.path.basename(file_path))[0]

                try:
                    # Preprocess the parquet file and insert into DB
                    logger.info(f"Processing file: {file_path}")
                    df = self.preprocess_parquet(file_path)
                    self.insert_into_db(df, table_name)
                    logger.info(f"Successfully processed and inserted {file_path} into table {table_name}.")
                except Exception as e:
                    # Log the error and continue processing other files
                    logger.error(f"Failed to process file {file_path} for table {table_name}. Error: {e}")
                    continue  # Continue with the next file

        except Exception as e:
            # Log any critical errors that prevent the overall process from completing
            logger.critical(f"Process failed. Error: {e}")

if __name__ == "__main__":

    print("========= Start inserting parquet into the database =========")
    processor = PARQUET_TO_DATABASE(data_folder="./data", db_path="NDW.db")
    processor.process()

    print("============ O .O ============")
    print("========= COMPLETED SAVED TO DATABASE =========")