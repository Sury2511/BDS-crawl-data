import undetected_chromedriver as uc 
from bs4 import BeautifulSoup
from datetime import datetime
from selenium.webdriver.common.by import By 
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import os


driver = uc.Chrome(version_main=121)
first_url = "https://batdongsan.com.vn/nha-dat-ban-tp-hcm"
url = 'https://batdongsan.com.vn/nha-dat-ban-tp-hcm/p{}?sortValue=1'
page_num = 1
columns_headers = ['title','address','price','area','price_per_m2', 'bedrooms','toilets','date']
file_exists = os.path.isfile('bds-hcm.xlsx')

if not file_exists:
    with pd.ExcelWriter('bds-hcm.xlsx', engine='openpyxl') as writer:
        pd.DataFrame(columns=columns_headers).to_excel(writer, sheet_name='Sheet1', index=False)

startrow = 0
while True:
    if page_num == 1:
        driver.get(first_url)
        dropdown_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, ".js__bds-select-button"))
        )

        dropdown_button.click()
        newest_option = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//li[contains(@class, 'js__option')][@vl='1']"))
        )
        newest_option.click()
    else:
        driver.get(url.format(page_num))
    
    html = driver.page_source      
    soup = BeautifulSoup(html, 'html.parser')    
    
    topics = soup.find_all(class_="re__card-info")
    exit_while = False
    property_data = []
    
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
        # if date_obj != datetime.now().date():
        #     exit_while = True
        #     break
        
        property_data.append([title,district,price,area,price_per_m2,bedrooms,toilets,day])

    
    if exit_while: 
        break
    
    df = pd.DataFrame(property_data)
    with pd.ExcelWriter('bds-hcm.xlsx', engine='openpyxl', mode='a', if_sheet_exists='overlay') as writer:
        startrow = 0
        if 'Sheet1' in writer.sheets:
            startrow = writer.sheets['Sheet1'].max_row
        df.to_excel(writer, sheet_name='Sheet1', index=False, header=None, startrow=startrow)

    print(page_num)      
    page_num += 1    
    if page_num > 2000:
        break
    
driver.quit()