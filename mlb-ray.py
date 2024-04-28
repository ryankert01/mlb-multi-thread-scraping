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
import ray



terms = ("player_hitting", "player_pitching", "team_hitting", "team_pitching")

@ray.remote
class MLBScraper:
    def __init__(self) -> None:
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


    def restart_driver(self) -> None:
        self.driver.quit()
        self.new_instance()
    
    def click_button(self, xpath: str) -> bool:
        for i in range(3):
            try:
                WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.XPATH, xpath)))
                button = self.driver.find_element(By.XPATH, xpath)
                if button.is_enabled():
                    button.click()
                    time.sleep(3)

                WebDriverWait(self.driver, 10).until(
                    lambda driver: "selected" in button.get_attribute("class")
                )

                if "selected" in button.get_attribute("class"):
                    return True
                else:
                    raise Exception(f"Failed to press {xpath}, try again...")

            except:
                self.driver.refresh()

        print(f"Failed to press {xpath} after 3 attempts")
        return False

    
    def click_next_button(self) -> bool:
        try:
            # Find the Next button and click it
            next_button = self.driver.find_element(By.CSS_SELECTOR, "button[aria-label='next page button']")
            if next_button.is_enabled():
                next_button.click()
                time.sleep(3)
                return True
            else:
                return False
        except NoSuchElementException:
            print(f"No next button found")
            return False
        

    def get_row_data(self, row, idx: int) -> list[str]:
        th = row.find('th')
        index = th.select('div[class^=index-]')[0].text
        if (len(th.div.select('[class^=full-]'))) == 2:
            name = th.div.select('[class^=full-]')[0].text + " " + th.div.select('[class^=full-]')[1].text
        else:
            name = th.div.select('[class^=full-]')[0].text
        if idx < 2:
            position = th.div.select_one('div[class^=position-]').text
            row_data = [index, name, position]
        else:
            row_data = [index, name]
        cells = row.find_all('td')
        row_data = row_data + [cell.text.strip() for cell in cells]
        return row_data

    def get_header_data(self, headers, idx) -> list[str]:
        header = ["index"]
        for i in range(len(headers)):
            if i % 2 == 0:
                header.append(headers[i].text)
            if i == 0 and idx < 2:
                header.append("position")
        return header
    
    def get_table_data(self, idx) -> tuple[list, list[list]]:
        soup = BeautifulSoup(self.driver.page_source, 'lxml')
            
        table = soup.find('table', class_='bui-table')
        headers = table.thead.find_all('abbr')
        header = self.get_header_data(headers, idx)

        rows = table.tbody.find_all('tr')
        r_data = []
        for row in rows:
            row_data = self.get_row_data(row, idx)
            r_data.append(row_data)
        return header, r_data

    def scrape_table_data(self, year:int, idx: int, expand=False) -> tuple[list, list[list]]:
        data = []
        page = 1
        while True:
            # Wait for the table to load
            self.driver.get(get_url(year, terms[idx], page=page))
            WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.bui-table")))

            if expand:
                btn_xpath = "//*[@id='stats-app-root']/section/section/div[1]/div[2]/div/div[1]/div/div[2]/button"
                if self.click_button(btn_xpath) == False:
                    raise Exception(f"Failed to press expand, restarting the driver... (re-run all tasks in {year})")

            
            try:
                header, r_data = self.get_table_data(idx)
                data += r_data
            except:
                print("Failed to scrape data, retrying...")
                self.driver.refresh()
                continue
            
            if self.click_next_button() == False:
                break
            page += 1

        return header, data


    def parse_year_data(self, year):
        year = str(year)
        folder = f"./mlb/{year}"
        if not os.path.exists(folder):
            os.makedirs(folder)

        for i in range(4):
            print("Scrapping data for", year, " ", terms[i])
            self.driver.get(get_url(year, terms[i]))
            header, data = self.scrape_table_data(year, i)
            header1, data1 = self.scrape_table_data(year, i, expand=True)
            
            print(len(header), len(header1))
            print(len(data), len(data1))

            header, data = aggregate_data(header, data, header1, data1, i)


            filename = f"{folder}/{terms[i]}.csv"

            with open(filename, mode='w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(header)
                writer.writerows(data)

        self.driver.quit()  # Don't forget to close the driver

    def safe_parse_year_data(self, year) -> None:
        while True:
            try:
                self.parse_year_data(year)
                break
            except:
                print(f"Failed to scrape data for {year}")
                traceback.print_exc()
                self.restart_driver()
        return True



if __name__ == "__main__":
    y_start = 2003
    y_end = 2024
    batch_size = 3
    for y in range(y_start, y_end, batch_size):
        ray.init()
        scrapers = [MLBScraper.remote() for i in range(batch_size)]
        s = [scrapers[i].safe_parse_year_data.remote(y+i) for i in range(batch_size)]
        ray.get(s)
        ray.shutdown()

