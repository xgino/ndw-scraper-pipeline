import os
import zipfile
import tarfile
import gzip
import shutil
import glob
import logging
from datetime import datetime
from tqdm import tqdm
from pathlib import Path

# Setup logger with an absolute path
log_file = Path(__file__).resolve().parent.parent / 'logs' / 'extract_downloads.log'
log_file.parent.mkdir(parents=True, exist_ok=True)  # Ensure the logs directory exists

# Setup  logger
logging.basicConfig(
    filename=str(log_file),
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('extract_downloads')

class FileExtractor:
    def __init__(self, download_folder=None, preprocess_folder='./extracted_downloads'):
        # Set default download folder path relative to the current script's directory
        self.download_folder = download_folder or os.path.join(os.path.dirname(__file__), '..', 'scrape_download', 'downloads')
        
        # Make preprocess_folder absolute, resolving relative paths based on the current script's directory
        self.preprocess_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), preprocess_folder))
        
        # Ensure the preprocess folder exists
        os.makedirs(self.preprocess_folder, exist_ok=True)

    def extract_file(self, filepath, target_folder):
        """
        Extract a file (.zip, .tar, .gz) to a specified folder.
        """
        try:
            if zipfile.is_zipfile(filepath):
                with zipfile.ZipFile(filepath, 'r') as zip_ref:
                    zip_ref.extractall(target_folder)
                logger.info(f"Extracted zip file: {filepath}")
            elif tarfile.is_tarfile(filepath):
                with tarfile.open(filepath, 'r') as tar_ref:
                    tar_ref.extractall(target_folder)
                logger.info(f"Extracted tar file: {filepath}")
            elif filepath.endswith('.gz'):
                extracted_file = os.path.join(
                    target_folder, os.path.basename(filepath).replace('.gz', '')
                )
                with gzip.open(filepath, 'rb') as f_in, open(extracted_file, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
                logger.info(f"Extracted gz file: {filepath} to {extracted_file}")
            else:
                logger.warning(f"Unsupported file format: {filepath}")
        except Exception as e:
            logger.error(f"Error extracting {filepath}: {e}")

    def organize_files(self):
        """
        Organize all data files into the preprocess folder.
        """
        for root, dirs, files in os.walk(self.preprocess_folder):
            for file in files:
                if file.endswith(('.json', '.geojson', '.csv', '.shp', '.parquet')):
                    src_file = os.path.join(root, file)
                    dest_folder = os.path.join(self.preprocess_folder, os.path.basename(root))
                    os.makedirs(dest_folder, exist_ok=True)
                    dest_file = os.path.join(dest_folder, file)
                    shutil.move(src_file, dest_file)
                    logger.info(f"Moved {file} to {dest_folder}")

        # Cleanup empty folders
        for root, dirs, _ in os.walk(self.preprocess_folder, topdown=False):
            for d in dirs:
                folder_path = os.path.join(root, d)
                if not os.listdir(folder_path):
                    os.rmdir(folder_path)
                    logger.info(f"Deleted empty folder: {folder_path}")

    def process_downloaded_files(self):
        """
        Main function to process downloaded files.
        """
        try:
            logger.info("Starting file processing")
            files_to_process = glob.glob(os.path.join(self.download_folder, '*'))

            for file in tqdm(files_to_process, desc="Processing files", unit="file"):
                if os.path.isfile(file) and file.endswith(('.zip', '.gz', '.tar')):
                    # Create a folder named after the compressed file
                    folder_name = os.path.splitext(os.path.basename(file))[0]
                    target_folder = os.path.join(self.preprocess_folder, folder_name)
                    os.makedirs(target_folder, exist_ok=True)

                    # Extract the file
                    self.extract_file(file, target_folder)

            # Organize files into preprocess folder
            self.organize_files()

            logger.info("File processing completed successfully")
        except Exception as e:
            logger.error(f"Error during file processing: {e}")


if __name__ == "__main__":

    print("========= Start extracting downloads =========")
    extractor = FileExtractor()
    extractor.process_downloaded_files()

    print("============ O .O ============")
    print("========= COMPLETED EXTRACTING DATA =========")