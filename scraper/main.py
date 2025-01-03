import subprocess
import os
import shutil

class NDW_DATA_COLLECTION:
    def __init__(self):
        # Call each function to execute the scripts sequentially
        self.deletefiles()
        self.scrape_data()
        self.extract_downloads()
        self.convert_to_parquet()
        self.parquet_to_database()

    def deletefiles(self):
        # List of directories to clear
        directories = ['./downloads', './preprocess', './data']
        
        for directory in directories:
            # Check if the directory exists
            if os.path.exists(directory):
                # Loop through the files and delete them
                for filename in os.listdir(directory):
                    file_path = os.path.join(directory, filename)
                    try:
                        if os.path.isdir(file_path):
                            shutil.rmtree(file_path)  # Remove directories
                        else:
                            os.remove(file_path)  # Remove files
                        print(f"Deleted: {file_path}")
                    except Exception as e:
                        print(f"Error deleting {file_path}: {e}")
            else:
                print(f"Directory {directory} does not exist.")

    def scrape_data(self):
        print("Starting data scrape...")
        self._run_script("scrape_opendata.py")
    
    def extract_downloads(self):
        print("Extracting downloaded files...")
        self._run_script("process_downloads.py")

    def convert_to_parquet(self):
        print("Converting files to Parquet...")
        self._run_script("xml_to_parquet.py")
        self._run_script("shp_geo_json_to_parquet.py")

    def parquet_to_database(self):
        print("Inserting data into database...")
        self._run_script("parquet_to_database.py")

    def _run_script(self, script_name):
        try:
            result = subprocess.run(["python", script_name], check=True, text=True, capture_output=True)
            print(f"Successfully ran {script_name}:\n{result.stdout}")
        except subprocess.CalledProcessError as e:
            print(f"Error running {script_name}:\n{e.stderr}")
            raise

if __name__ == "__main__":
    print("==================")
    print("========= START RDW DATA COLLECTION =========")
    print("==================")

    NDW_DATA_COLLECTION() 

    print("==================")
    print("========= COMPLETED RDW DATA COLLECTION =========")
    print("==================")