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

from utils import get_batch_urls

def aggregate_data(header1, data1, header2, data2, shift):
    assert len(data1) == len(data2), f"data1 and data2 must have the same length, got {len(data1)} and {len(data2)} respectively"
    j = 4 if shift < 2 else 3
    header, data = header1 + header2[j:], [data1[i] + data2[i][j:] for i in range(len(data1))]
    if shift == 0:
        assert len(header) == 35
    elif shift == 1:
        assert len(header) == 41
    elif shift == 2:
        assert len(header) == 30
    elif shift == 3:
        assert len(header) == 37
    return header, data



def scrape_table_data(driver, idx, expand=False):
    data = []
    while True:
        # Wait for the table to load
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.bui-table")))

        if expand:
            try:
                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//*[@id='stats-app-root']/section/section/div[1]/div[2]/div/div[1]/div/div[2]/button")))
                next_button = driver.find_element(By.XPATH, "//*[@id='stats-app-root']/section/section/div[1]/div[2]/div/div[1]/div/div[2]/button")
                if next_button.is_enabled():
                    next_button.click()
                    time.sleep(3)
                else:
                    break
            except:
                print("Failed to press expand")
                traceback.print_exc()
                break
        
        soup = BeautifulSoup(driver.page_source, 'lxml')
        
        table = soup.find('table', class_='bui-table')
        headers = table.thead.find_all('abbr')
        header = ["index"]
        for i in range(len(headers)):
            if i % 2 == 0:
                header.append(headers[i].text)
            if i == 0 and idx < 2:
                header.append("position")



        rows = table.tbody.find_all('tr')
        
        for row in rows:
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
            data.append(row_data)
        
        try:
            # Find the Next button and click it
            next_button = driver.find_element(By.CSS_SELECTOR, "button[aria-label='next page button']")
            if next_button.is_enabled():
                next_button.click()
                time.sleep(3)
            else:
                break
        except NoSuchElementException:
            break

    return header, data


def parse_year_data(year):
    options = Options()
    # options.add_argument("--headless")  # Enable headless mode for background processing
    driver = webdriver.Chrome(options=options)

    driver.get("https://www.mlb.com/stats/2021")
    current_time = datetime.datetime.now().isoformat() + 'Z' 

    # Create a cookie
    cookie = {
        'name': 'OptanonAlertBoxClosed',
        'value': current_time,
        'domain': '.mlb.com',  # Set this to the domain for which you want the cookie
        'path': '/',
    }
    driver.add_cookie(cookie)

    year = str(year)
    urls = get_batch_urls(year)
    terms = ["player_hitting", "player_pitching", "team_hitting", "team_pitching"]
    folder = f"./mlb/{year}"
    if not os.path.exists(folder):
        os.makedirs(folder)

    for i in range(4):
        driver.get(urls[i])
        header, data = scrape_table_data(driver,i)
        driver.get(urls[i])
        try:
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//*[@id='stats-app-root']/section/section/div[1]/div[2]/div/div[1]/div/div[2]/button")))
            next_button = driver.find_element(By.XPATH, "//*[@id='stats-app-root']/section/section/div[1]/div[2]/div/div[1]/div/div[2]/button")
            if next_button.is_enabled():
                next_button.click()
                time.sleep(3)
            else:
                break
        except:
            print("Failed to press expand")
            traceback.print_exc()
            break
        header1, data1 = scrape_table_data(driver,i)
        print("Scraped data for", year, " ", terms[i])
        print(len(header), len(header1))
        print(len(data), len(data1))

        header, data = aggregate_data(header, data, header1, data1,i)


        filename = f"{folder}/{terms[i]}.csv"

        with open(filename, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(header)
            writer.writerows(data)

    driver.quit()  # Don't forget to close the driver

def main():
    year = 2003
    while year <= 2023:
        try:
            parse_year_data(year)  
        except:
            print(f"Failed to scrape data for {year}")
            traceback.print_exc()
            continue
        year += 1

if __name__ == "__main__":
    main()
