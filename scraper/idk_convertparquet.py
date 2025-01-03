import os
import pandas as pd
import geopandas as gpd
import xmltodict
from tqdm import tqdm

class FileToParquetConverter:
    def __init__(self, input_dir="./preprocess", output_dir="./data"):
        """
        Initialize the converter with input and output directories.
        Args:
            input_dir: Directory to search for files (default: "./preprocess").
            output_dir: Directory to save Parquet files (default: "./data").
        """
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.ensure_output_dir()
    
    def ensure_output_dir(self):
        """
        Create the output directory if it does not exist.
        """
        os.makedirs(self.output_dir, exist_ok=True)
    
    def clean_dict(self, data, parent_key=""):
        """
        Flatten and clean XML/JSON dictionary into a flat structure with uniform types.
        """
        items = {}
        for k, v in data.items():
            new_key = f"{parent_key}.{k}" if parent_key else k
            if isinstance(v, dict):
                items.update(self.clean_dict(v, new_key))
            elif isinstance(v, list):
                items[new_key] = str(v)  # Convert lists to string for simplicity
            else:
                items[new_key] = v if v is not None else ""
        return items
    
    def parse_xml_file(self, xml_file, output_parquet):
        """
        Parse XML file and convert to Parquet.
        """
        try:
            with open(xml_file, "r", encoding="utf-8") as file:
                xml_data = xmltodict.parse(file.read())
            
            # Flatten the nested dictionary
            flattened_data = self.clean_dict(xml_data)
            df = pd.DataFrame([flattened_data])
            
            # Save to Parquet
            df.to_parquet(output_parquet, engine="pyarrow", index=False)
            print(f"Processed: {xml_file} -> {output_parquet}")
        except Exception as e:
            print(f"Error processing {xml_file}: {e}")
    
    def parse_other_file(self, file_path, output_parquet):
        """
        Parse other formats (CSV, GeoJSON, SHP, JSON) and convert to Parquet.
        """
        try:
            if file_path.endswith(".csv"):
                df = pd.read_csv(file_path)
            elif file_path.endswith(".geojson"):
                df = gpd.read_file(file_path)
            elif file_path.endswith(".shp"):
                df = gpd.read_file(file_path)
            elif file_path.endswith(".json"):
                df = pd.read_json(file_path)
            else:
                print(f"Unsupported file format: {file_path}")
                return
            
            # Save to Parquet
            df.to_parquet(output_parquet, engine="pyarrow", index=False)
            print(f"Processed: {file_path} -> {output_parquet}")
        except Exception as e:
            print(f"Error processing {file_path}: {e}")
    
    def process_all_files(self):
        """
        Process all supported files in the input directory (including subdirectories).
        """
        supported_extensions = (".xml", ".csv", ".shp", ".geojson", ".json", ".gpkg")
        file_groups = {}

        # Group files by folder name and handle duplicates
        for root, _, files in os.walk(self.input_dir):
            folder_name = os.path.basename(root)
            for file in files:
                if file.endswith(supported_extensions):
                    file_path = os.path.join(root, file)
                    base_name = os.path.splitext(file)[0]

                    if base_name not in file_groups:
                        file_groups[base_name] = {}

                    ext = file.split(".")[-1]
                    file_groups[base_name][ext] = file_path

        # Process files based on priority
        for base_name, files in tqdm(file_groups.items(), desc="Processing files"):
            output_path = os.path.join(self.output_dir, f"{base_name}.parquet")
            if "json" in files:
                self.parse_other_file(files["json"], output_path)
            elif "csv" in files:
                self.parse_other_file(files["csv"], output_path)
            elif "xml" in files:
                self.parse_xml_file(files["xml"], output_path)
            else:
                for ext in ["shp", "geojson", "gpkg"]:
                    if ext in files:
                        self.parse_other_file(files[ext], output_path)
                        break


if __name__ == "__main__":
    converter = FileToParquetConverter()
    converter.process_all_files()