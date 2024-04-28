from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from bs4 import BeautifulSoup
import datetime
import time
import os
import csv
import traceback

from utils import get_url, aggregate_data
from mlb import MLBScraper
import ray



terms = ("player_hitting", "player_pitching", "team_hitting", "team_pitching")

@ray.remote
class MLBRemoteScraper(MLBScraper):
    def __init__(self):
        self.new_instance()

    def new_instance(self) -> None:
        self.options = Options()
        self.options.add_argument("--headless")  # Enable headless mode for background processing
        self.driver = webdriver.Chrome(options=self.options)
        self.current_time = datetime.datetime.now().isoformat() + 'Z' 
        self.driver.get("https://www.mlb.com/stats/2021")
        # Create a cookie
        self.cookie = {
            'name': 'OptanonAlertBoxClosed',
            'value': self.current_time,
            'domain': '.mlb.com',  # Set this to the domain for which you want the cookie
            'path': '/',
        }
        self.driver.add_cookie(self.cookie)



if __name__ == "__main__":
    y_start = 2003
    y_end = 2024
    batch_size = 7
    assert batch_size <= y_end - y_start, "Batch size must be less than the range of years"
    assert (y_end - y_start) % batch_size == 0, "Range of years must be divisible by batch size"
    for y in range(y_start, y_end, batch_size):
        ray.init()
        scrapers = [MLBRemoteScraper.remote() for i in range(batch_size)]
        s = [scrapers[i].safe_parse_year_data.remote(y+i) for i in range(batch_size)]
        ray.get(s)
        ray.shutdown()

