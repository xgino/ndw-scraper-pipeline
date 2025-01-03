
import pandas as pd 
import numpy as np 
import geopandas as gpd 
import os

import logging
# Setup  logger
logging.basicConfig(
    filename='./logs/to_parquet.log', 
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('to_parquet')

class CONVERT_TO_PARQUET:
    def __init__(self):
        self.input_folder = "./preprocess/"
        self.output_folder = "./data/"

        self.get_ndw_data()
        #self.get_verkeerboard_rd_data() # basicly the same
        #self.get_verkeerboard_wgs_data()
        self.get_verkeersborden_actueel_beeld_data()

    def get_ndw_data(self):
        try:
            logger.info("Starting NDW data conversion.")
            folder = os.path.join(self.input_folder, "NDW_AVG_Meetlocaties_Shapefile")
            
            # Get the meetfile and telpunt file names that start with 'Meet' and 'Tel' and end with '.shp'
            meetfile = next((f for f in os.listdir(folder) if f.startswith('Meet') and f.endswith('.shp')), None)
            telpunt = next((f for f in os.listdir(folder) if f.startswith('Tel') and f.endswith('.shp')), None)
            if not meetfile or not telpunt:
                raise FileNotFoundError("Required shapefiles not found in the NDW folder.")

            # Read Shape files
            meetpunt_df = gpd.read_file(os.path.join(folder, meetfile))
            telpunt_df = gpd.read_file(os.path.join(folder, telpunt))
        
            # Create output folder 
            output_folder = os.path.join(self.output_folder, "NDW_AVG_Meetlocaties_Shapefile")
            os.makedirs(output_folder, exist_ok=True)
            meetpunt_df.to_parquet(os.path.join(output_folder, meetfile.split('.')[0] + '.parquet'))
            telpunt_df.to_parquet(os.path.join(output_folder, telpunt.split('.')[0] + '.parquet'))
            
            logger.info(f"Completed NDW data conversion to Parquet. Output folder: {output_folder}")
            print(f"Completed NDW data conversion to Parquet. Output folder: {output_folder}")
        except Exception as e:
            logger.error(f"Error processing NDW data: {e}")
            print(f"Error processing NDW data: {e}")
        

    def get_verkeerboard_rd_data(self):
        try:
            logger.info("Starting verkeersborden_actueel_beeld_rd data conversion.")
            rd_folder = os.path.join(self.input_folder, "verkeersborden_actueel_beeld_rd.geojson")
            rd_file = next((f for f in os.listdir(rd_folder) if f.startswith('verkeersborden') and f.endswith('.geojson')), None)
            if not rd_file:
                raise FileNotFoundError("RD geojson file not found.")

            # Read file and save to Parquet
            rd_file_df = gpd.read_file(os.path.join(rd_folder, rd_file))
            rd_file_df.to_parquet(os.path.join(self.output_folder, rd_file.split('.')[0] + '.parquet'))

            logger.info(f"Completed RD data conversion to Parquet. File saved at: {output_path}")
        except Exception as e:
            logger.error(f"Error processing verkeersborden_actueel_beeld_rd.geojson: {e}")
            print(f"Error processing verkeersborden_actueel_beeld_rd.geojson: {e}")


    def get_verkeerboard_wgs_data(self):
        try:
            logger.info("Starting verkeersborden_actueel_beeld_wgs84 data conversion.")
            wgs84_folder = os.path.join(self.input_folder, "verkeersborden_actueel_beeld_wgs84.geojson")
            wgs84_file = next((f for f in os.listdir(wgs84_folder) if f.startswith('verkeersborden') and f.endswith('.geojson')), None)
            if not wgs84_file:
                raise FileNotFoundError("wgs84 geojson file not found.")
        
            # Read file and save to Parquet
            wgs84_file_df = gpd.read_file(os.path.join(wgs84_folder, wgs84_file))
            wgs84_file_df.to_parquet(os.path.join(self.output_folder, wgs84_file.split('.')[0] + '.parquet'))
            logger.info(f"Completed wgs84 data conversion to Parquet. File saved at: {output_path}")
        except Exception as e:
            logger.error(f"Error processing verkeersborden_actueel_beeld_wgs84.geojson: {e}")
            print(f"Error processing verkeersborden_actueel_beeld_wgs84.geojson: {e}")


    def get_verkeersborden_actueel_beeld_data(self):
        try:
            logger.info("Starting verkeersborden_actueel_beeld data conversion.")
            verkeersborden_folder = os.path.join(self.input_folder, "verkeersborden_actueel_beeld.json")
            verkeersborden_file = next((f for f in os.listdir(verkeersborden_folder) if f.startswith('verkeersborden') and f.endswith('.json')), None)
            if not verkeersborden_file:
                raise FileNotFoundError("verkeersborden_actueel_beeld json file not found.")
        
            # Read file and save to Parquet
            verkeersborden_df = pd.read_json(os.path.join(verkeersborden_folder, verkeersborden_file))
            verkeersborden_df.to_parquet(os.path.join(self.output_folder, verkeersborden_file.split('.')[0] + '.parquet'))
            logger.info(f"Completed verkeersborden_actueel_beeld data conversion to Parquet. File saved at: {output_path}")
        except Exception as e:
            logger.error(f"Error processing verkeersborden_actueel_beeld.json: {e}")
            print(f"Error processing verkeersborden_actueel_beeld.json: {e}")




if __name__ == "__main__":
    print("========= Start converting other files into parquet =========")
    CONVERT_TO_PARQUET()

    print("============ O .O ============")
    print("========= COMPLETED OTHER TO PARQUET =========")