import undetected_chromedriver as uc
import time 

from bs4 import BeautifulSoup
import time
import re
from selenium.webdriver.common.by import By 
from datetime import date

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

driver = uc.Chrome(version_main=121)
base_url ='https://batdongsan.com.vn/nha-dat-ban-tp-hcm/p{}'
page_num = 1
data = []
while True:
    current_page_url = base_url.format(page_num)
    print(current_page_url)
    driver.get(current_page_url)
    time.sleep(3)
        
    html = driver.page_source
    soup = BeautifulSoup(html, 'html.parser')
    card_info_content = soup.find_all('div', class_='re__card-info-content')
    page_data = []
    for card in card_info_content:
        title = card.find('span', class_='pr-title js__card-title')
        price = card.find('span', class_='re__card-config-price js__card-config-item')
        acreage = card.find('span', class_='re__card-config-area js__card-config-item')
        price_per_acreage = card.find('span', class_='re__card-config-price_per_m2 js__card-config-item')
        bedroom = extract_number_from_element(html,'re__card-config-bedroom js__card-config-item')
        toilet = extract_number_from_element(html,'re__card-config-toilet js__card-config-item')
        address_div = card.find('div', class_='re__card-location')
        address = address_div.find_all('span')
        data.append([clean_text(title.text), price.text, acreage.text, price_per_acreage.text, bedroom, toilet, address[1].text])

    
    print(data)
        
    page_num += 1    
    if page_num > 10:
        break
    
driver.quit()