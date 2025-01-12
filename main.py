import subprocess
import os
import shutil

class NDW_DATA_COLLECTION:
    def __init__(self):
        self.scrape_data()
        self.extract_downloads()
        self.convert_to_parquet()
        self.parquet_to_database()
        self.deletefiles()

    def scrape_data(self):
        from scrape_download.scrape_rdw_opendata import WebScraper
        BASE_URL = "https://opendata.ndw.nu"
        PROXY_FILE = "socks5.txt"

        print("========= Start scraping data from NDW =========")

        scraper = WebScraper(base_url=BASE_URL, proxy_file=PROXY_FILE)
        scraper.run()

        print("============ O .O ============")
        print("========= COMPLETED SCRAPING =========")
    
    def extract_downloads(self):
        from extract_downloads.extract_downloads import FileExtractor
        
        print("========= Start extracting downloads =========")
        extractor = FileExtractor()
        extractor.process_downloaded_files()

        print("============ O .O ============")
        print("========= COMPLETED EXTRACTING DATA =========")

    def convert_to_parquet(self):
        print("Converting files to Parquet...")
        self.output_folder = os.path.join(os.path.dirname(__file__), 'convert_to_parquet', 'parquet')
        os.makedirs(self.output_folder, exist_ok=True)

        def process_simple_xml(input, output, name):
            try:
                print(f"Starting: '{name}'")
                from convert_to_parquet.simple_xml_to_parquet import MultiNestedXMLToParquet
                converter = MultiNestedXMLToParquet()
                parsed_data = converter.parse_xml_to_dict(input)
                converter.convert_to_parquet(parsed_data, output)

                print(f"Completed '{name}'")
            except Exception as e:
                print(f"Error in '{name}': {e}")


        process_simple_xml(input='./extract_downloads/extracted_downloads/actuele_statusberichten.xml/actuele_statusberichten.xml', output='./convert_to_parquet/parquet/actuele_statusberichten.parquet', name='actuele_statusberichten')
        process_simple_xml(input='./extract_downloads/extracted_downloads/brugopeningen.xml/brugopeningen.xml', output='./convert_to_parquet/parquet/brugopeningen.parquet', name='brugopeningen')
        process_simple_xml(input='./extract_downloads/extracted_downloads/incidents.xml/incidents.xml', output='./convert_to_parquet/parquet/incidents.parquet', name='incidents')
        process_simple_xml(input='./extract_downloads/extracted_downloads/wegwerkzaamheden.xml/wegwerkzaamheden.xml', output='./convert_to_parquet/parquet/wegwerkzaamheden.parquet', name='wegwerkzaamheden')
            
        def Matrixsignaalinformatie():
            try:
                print(f"Starting: 'Matrixsignaalinformatie'")
                from convert_to_parquet.matrixsignaal_xml_to_parquet import MultiNestedXMLToParquet
                xml_file_path = "./extract_downloads/extracted_downloads/Matrixsignaalinformatie.xml/Matrixsignaalinformatie.xml"
                output_file = "./convert_to_parquet/parquet/Matrixsignaalinformatie.parquet"

                converter = MultiNestedXMLToParquet()
                parsed_data = converter.parse_xml_to_dict(xml_file_path)
                converter.convert_to_parquet(parsed_data, output_file)
                print(f"Completed 'Matrixsignaalinformatie'")
            except Exception as e:
                print(f"Error in 'Matrixsignaalinformatie': {e}")
       
        def trafficspeed():
            try:
                print(f"Starting: 'trafficspeed'")
                from convert_to_parquet.trafficspeed_xml_to_parquet import MultiNestedXMLToParquet
                xml_file_path = "./extract_downloads/extracted_downloads/trafficspeed.xml/trafficspeed.xml"
                output_file = "./convert_to_parquet/parquet/trafficspeed.parquet"

                converter = MultiNestedXMLToParquet()
                parsed_data = converter.parse_xml_to_dict(xml_file_path)
                converter.convert_to_parquet(parsed_data, output_file)
                print(f"Completed 'trafficspeed'")
            except Exception as e:
                print(f"Error in 'trafficspeed': {e}")

        def traveltime():
            try:
                print(f"Starting: 'traveltime'")
                from convert_to_parquet.traffictime_xml_to_parquet import MultiNestedXMLToParquet
                xml_file_path = "./extract_downloads/extracted_downloads/traveltime.xml/traveltime.xml"
                output_file = "./convert_to_parquet/parquet/traveltime.parquet"

                converter = MultiNestedXMLToParquet()
                parsed_data = converter.parse_xml_to_dict(xml_file_path)
                converter.convert_to_parquet(parsed_data, output_file)
                print(f"Completed 'traveltime'")
            except Exception as e:
                print(f"Error in 'traveltime': {e}")
            
        def other():
            try:
                print(f"Starting: 'NDW Meetvakken & telpunten, verkeersborden actueel'")
                from convert_to_parquet.shp_geo_json_to_parquet import CONVERT_TO_PARQUET
                # this converts NDW Meetvakken & telpunten, verkeersborden actueel beeld
                CONVERT_TO_PARQUET()
                print(f"Completed 'NDW Meetvakken & telpunten, verkeersborden actueel'")
            except Exception as e:
                print(f"Error in 'NDW Meetvakken & telpunten, verkeersborden actueel': {e}")
     
            
        Matrixsignaalinformatie()
        trafficspeed()
        traveltime()
        other()

    def parquet_to_database(self):
        print("Inserting data into database...")
        print("========= Start inserting parquet into the database =========")
        from parquet_to_database.parquet_to_database import PARQUET_TO_DATABASE
        processor = PARQUET_TO_DATABASE(data_folder="./convert_to_parquet/parquet", db_path="./NDW.db", process_date=None) #eg. format: process_date="22-12-2024" None is today
        processor.process()

        print("============ O .O ============")
        print("========= COMPLETED SAVED TO DATABASE =========")

    def deletefiles(self):
        # List of directories to clear
        directories = ['./scrape_download/downloads', './extract_downloads/extracted_downloads', './convert_to_parquet/parquet']
        
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

if __name__ == "__main__":
    print("==================")
    print("========= START RDW DATA COLLECTION =========")
    print("==================")

    NDW_DATA_COLLECTION() 

    print("==================")
    print("========= COMPLETED RDW DATA COLLECTION =========")
    print("==================")
