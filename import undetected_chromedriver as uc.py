import undetected_chromedriver as uc
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By 
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import os
import time
import threading
from datetime import datetime

# Function to process a set of pages in a single thread
def process_pages(thread_id, num_threads, total_pages, data_lock, property_data,driver):
    for page_num in range(thread_id, total_pages + 1, num_threads):
        print(page_num)
        if page_num == 1:
            url = "https://batdongsan.com.vn/nha-dat-ban-tp-hcm"
            driver.get(url)
            dropdown_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, ".js__bds-select-button"))
            )
            dropdown_button.click()
            newest_option = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//li[contains(@class, 'js__option')][@vl='1']"))
            )
            newest_option.click()
        else:
            url = 'https://batdongsan.com.vn/nha-dat-ban-tp-hcm/p{}?sortValue=1'.format(page_num)
            driver.get(url)
        
        
        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')
        topics = soup.find_all(class_="re__card-info")
        exit_while = False
        
        page_data = []
        for topic in topics:
            title = topic.find('span', class_='pr-title js__card-title').get_text(strip=True) if topic.find('span', class_='pr-title js__card-title') else ''      
            address = topic.find('div', class_='re__card-location').find('span').get_text(strip=True) if topic.find('div', class_='re__card-location') else ''
            district = address.split(',')[0].strip()
            price  = topic.find(class_='re__card-config-price js__card-config-item').get_text(strip=True) if topic.find(class_='re__card-config-price js__card-config-item') else ''        
            area  = topic.find(class_='re__card-config-area js__card-config-item').get_text(strip=True) if topic.find(class_='re__card-config-area js__card-config-item') else ''        
            price_per_m2  = topic.find(class_='re__card-config-price_per_m2 js__card-config-item').get_text(strip=True) if topic.find(class_='re__card-config-price_per_m2 js__card-config-item') else ''
            bedrooms  = topic.find(class_='re__card-config-bedroom js__card-config-item')['aria-label'] if topic.find(class_='re__card-config-bedroom js__card-config-item') else ''
            toilets  = topic.find(class_='re__card-config-toilet js__card-config-item')['aria-label'] if topic.find(class_='re__card-config-toilet js__card-config-item') else ''
            day = topic.find('span',class_='re__card-published-info-published-at')['aria-label'] if topic.find('span',class_='re__card-published-info-published-at') else ''
            date_obj = datetime.strptime(day, "%d/%m/%Y").date()
            
            #Lấy dữ liệu lần đầu thì command điều kiện if này
            if date_obj != datetime.now().date():
                exit_while = True
                break
            page_data.append([title, district, price, area, price_per_m2, bedrooms, toilets, day])
        
        if exit_while:
            break

        # Lock to safely update the shared data structure
        with data_lock:
            property_data.extend(page_data)

    driver.quit()

def main():
    
    columns_headers = ['title', 'address', 'price', 'area', 'price_per_m2', 'bedrooms', 'toilets', 'date']
    file_exists = os.path.isfile('bds-hcm.xlsx')

    if not file_exists:
        with pd.ExcelWriter('bds-hcm.xlsx', engine='openpyxl') as writer:
            pd.DataFrame(columns=columns_headers).to_excel(writer, sheet_name='Sheet1', index=False)

    property_data = []
    data_lock = threading.Lock()

    # Setup Chrome options for undetected_chromedriver

    num_threads = 4  # Number of chrome open
    total_pages = 100  # Total number of pages to scrape

    threads = []
    for thread_id in range(num_threads):
        driver = uc.Chrome(version_main=121)
        t = threading.Thread(target=process_pages, args=(thread_id, num_threads, total_pages, data_lock, property_data,driver))
        threads.append(t)
        t.start()

    for thread in threads:
        thread.join()

    # Save to Excel
    df = pd.DataFrame(property_data)
    with pd.ExcelWriter('bds-hcm.xlsx', engine='openpyxl', mode='a', if_sheet_exists='overlay') as writer:
        startrow = writer.sheets['Sheet1'].max_row if 'Sheet1' in writer.sheets else 0
        df.to_excel(writer, sheet_name='Sheet1', index=False, header=None, startrow=startrow)

if __name__ == "__main__":
    start = time.time()
    main()
    end = time.time()
    print(end-start)
