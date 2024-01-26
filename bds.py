from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

from bs4 import BeautifulSoup
import time
import random
import re

def extract_number_from_element(html, class_name):
    soup = BeautifulSoup(html, 'html.parser')
    element = soup.find('span', class_=class_name)
    if element:
        # If the number is in the text of the span
        number = element.text.strip()
        
        # If you need to extract just the numeric part from a string (e.g., '4 Phòng ngủ')
        number = re.search(r'\d+', number)
        return number.group(0) if number else None
    return None
def clean_text(line):
    return re.sub(r'\s+', ' ', line).strip()

def get_data_from_current_page(driver):
    html = driver.page_source
    soup = BeautifulSoup(html, 'html.parser')
    card_info_content = soup.find_all('div', class_='AdBody_AdBody__o2J9r')
    page_data = []
    for card in card_info_content:
        title = card.find('h3', class_='commonStyle_adTitle__g520j')
        acreage = card.find('span', class_='AdBody_adItemCondition__ppptn')
        price = card.find('p', class_='AdBody_adPriceNormal___OYFU')
        if title == None:
            title = ""
        if acreage == None:
            acreage = ""
        if price == None:
            price = ""
        page_data.append([title.text,acreage.text,price.text])
    return page_data

chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.6167.86 Safari/537.36")
chrome_path = Service(r"D:\Đồ án\chromedriver.exe")
driver = webdriver.Chrome(service=chrome_path, options=chrome_options)

driver.implicitly_wait(10)

# Starting URL for the first page of listings
base_url = 'https://www.nhatot.com/mua-ban-bat-dong-san-tp-ho-chi-minh?page={}'
page_num = 1  # Start with page 1

all_data = []  # List to hold data from all pages

try:
    while True:
        # Construct the URL for the current page
        current_page_url = base_url.format(page_num)
        driver.get(current_page_url)

        # Close the pop-up by clicking on a safe area
        WebDriverWait(driver, 2).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        driver.find_element(By.TAG_NAME, "body").click()

        time.sleep(2)

        current_page_data = get_data_from_current_page(driver)
        all_data.extend(current_page_data)

        # Thêm độ trễ ngẫu nhiên sau mỗi yêu cầu
        time.sleep(10) # Chờ từ 5 đến 10 giây một cách ngẫu nhiên
        page_num += 1


except Exception as e:
    print(f"An error occurred: {e}")
finally:
    driver.quit()



# Now all_data contains data from all pages
print(all_data)

