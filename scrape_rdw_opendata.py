import os
import logging
from datetime import datetime, timedelta
from urllib.parse import urlparse
import urllib.robotparser
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from tqdm import tqdm
import random
import time
import json
from pathlib import Path

# Setup logger with an absolute path
log_file = Path(__file__).resolve().parent.parent / 'logs' / 'scrape_rdw_opendata.log'
log_file.parent.mkdir(parents=True, exist_ok=True)  # Ensure the logs directory exists

# Setup  logger
logging.basicConfig(
    filename=str(log_file), 
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('scrape_rdw_opendata')

class WebScraper:
    """
    Robust web scraper for downloading files from a table on a webpage.
    Respects robots.txt, rotates proxies and user agents, and logs downloads.
    """

    def __init__(self, base_url, proxy_file="socks5.txt"):
        self.base_url = base_url
        self.user_agent_rotator = UserAgent()
        self.proxies = self.load_proxies(proxy_file)
        
        # Use the absolute path of the script's directory for relative paths
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        self.download_dir = os.path.join(self.script_dir, "downloads")
        os.makedirs(self.download_dir, exist_ok=True)

    @staticmethod
    def load_proxies(proxy_file):
        """Load proxies from a file."""
        if not os.path.exists(proxy_file):
            return []
        with open(proxy_file, "r") as file:
            return [line.strip() for line in file.readlines()]

    def can_fetch(self, url):
        """Check if a URL can be fetched based on the robots.txt file."""
        parsed_url = urlparse(self.base_url)
        robots_url = f"{parsed_url.scheme}://{parsed_url.netloc}/robots.txt"
        rp = urllib.robotparser.RobotFileParser()
        rp.set_url(robots_url)
        try:
            rp.read()
        except Exception as e:
            logger.warning(f"Failed to read robots.txt: {e}")
            return False
        return rp.can_fetch("*", url)

    def fetch_page(self, url, max_retries=5):
        """Fetch the page content with retries, using IP and User-Agent rotation."""
        headers = {"User-Agent": self.user_agent_rotator.random}

        # Sample up to 5 random proxies for retries
        sampled_proxies = random.sample(self.proxies, min(5, len(self.proxies))) if self.proxies else []
        
        for proxy in sampled_proxies:
            for attempt in range(max_retries):
                proxies = {"http": f"http://{proxy}", "https": f"http://{proxy}"}
                try:
                    response = requests.get(url, headers=headers, proxies=proxies, timeout=20)
                    if response.status_code == 200:
                        logging.info(f"Success with proxy: {proxy} on attempt {attempt + 1}")
                        return response.text
                    logging.warning(f"Failed attempt {attempt + 1} with proxy: {proxy} (HTTP {response.status_code})")
                except Exception as e:
                    logging.error(f"Attempt {attempt + 1} failed with proxy: {proxy} ({e})")
                    break  # Stop retries for this proxy
            
        # Fallback to device's own network (no proxy)
        logging.info("Retrying with device's own network after exhausting all proxies.")
        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=headers, timeout=20)
                if response.status_code == 200:
                    logging.info(f"Success with device's own network on attempt {attempt + 1}")
                    return response.text
                logging.warning(f"Failed attempt {attempt + 1} with device's own network (HTTP {response.status_code})")
            except Exception as e:
                logging.error(f"Attempt {attempt + 1} failed with device's own network ({e})")
        
        logging.error("Failed to fetch the webpage with proxies and device's own network.")
        return None

    def extract_table_data(self, html_content):
        """Extract data from the first table and return a list of dictionaries."""
        soup = BeautifulSoup(html_content, "lxml")
        table = soup.find("table")
        if not table:
            logger.warning("No table found on the page.")
            print("No table found on the page.")
            return []

        data = []
        for row in table.find_all("tr"):
            cols = row.find_all("td")
            if not cols:
                continue
            name = cols[0].get_text(strip=True)
            link = cols[0].find("a")["href"] if cols[0].find("a") else ""
            last_modified = cols[1].get_text(strip=True) if len(cols) > 1 else ""
            size = cols[2].get_text(strip=True) if len(cols) > 2 else ""
            data.append({
                "Name": name,
                "Link": f"{self.base_url}/{link}" if link else "",
                "Last Modified": last_modified,
                "Size": size,
            })
        return data

    def download_file(self, file_info):
        """Download a file and log details such as time, size, and location."""
        name, link, last_modified = file_info["Name"], file_info["Link"], file_info["Last Modified"]

        # Save to the downloads directory inside the folder
        save_path = os.path.join(self.download_dir, name.replace(" ", "_"))
        os.makedirs(os.path.dirname(save_path), exist_ok=True)

        try:
            response = requests.get(link, stream=True, timeout=20)
            response.raise_for_status()
            
            # Get file size
            total_size = int(response.headers.get("content-length", 0))
            
            # Download file with progress bar
            with open(save_path, "wb") as f, tqdm(
                total=total_size,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                desc=name,
            ) as bar:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
                    bar.update(len(chunk))

            # Log success with details
            log_message = (
                f"Downloaded: {name}, Size: {total_size / (1024 * 1024):.2f} MB, "
                f"Folder: {os.path.abspath(save_path)}"
            )
            logging.info(log_message)
            print(log_message)
        except Exception as e:
            # Log failure
            error_message = f"Failed to download {name} from {link}: {e}"
            logging.error(error_message)
            print(error_message)

    def run(self):
        """Main scraping workflow."""
        print("========= Start Scraping NDW OpenData =========")

        if not self.can_fetch(self.base_url):
            logger.error("Blocked by robots.txt.")
            print("Blocked by robots.txt.")
            return

        html_content = self.fetch_page(self.base_url)
        if not html_content:
            logger.error("Failed to fetch the webpage.")
            print("Failed to fetch the webpage.")
            return

        table_data = self.extract_table_data(html_content)
        for file_info in table_data:
            self.download_file(file_info)

        print("========= Finished Scraping =========")
        print("========= o .o =========")


if __name__ == "__main__":
    BASE_URL = "https://opendata.ndw.nu"
    PROXY_FILE = "socks5.txt"

    print("========= Start scraping data from NDW =========")

    scraper = WebScraper(base_url=BASE_URL, proxy_file=PROXY_FILE)
    scraper.run()

    print("============ O .O ============")
    print("========= COMPLETED SCRAPING =========")