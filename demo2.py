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
from selenium.common.exceptions import TimeoutException
from sqlalchemy import create_engine,text
import mysql.connector
import mysql

check_Database_Emty=True

def connect_My_SQL(user,psw,host,port,database,table_name):
    engine = create_engine('mysql+mysqlconnector://{}:{}@{}:{}/{}'.format(user,psw,host,port,database))
    engine.connect()
    check_day=pd.read_sql("SELECT MAX(date) as MAX_DATE FROM {}".format(table_name),con=engine)
    check_day=check_day["MAX_DATE"][0]
    if check_day == None:
        check_Database_Emty=True
    else:
        check_Database_Emty=False
    return engine

def write_Data_To_Database(df,engine,table_name):
    df.to_sql(name=table_name,con=engine,index=True,if_exists='append',index_label='id')

def process_pages(thread_id, num_threads, total_pages, data_lock, property_data,driver,url):
    last_successful_page = thread_id - num_threads
    while last_successful_page <= total_pages:
        try:
            for page_num in range(last_successful_page + num_threads, total_pages + 1, num_threads):
                print(page_num)
                if page_num == 1:
                    if url == hcm_url:
                        driver.get("https://batdongsan.com.vn/nha-dat-ban-tp-hcm")
                    elif url == hn_url:
                        driver.get("https://batdongsan.com.vn/nha-dat-ban-ha_noi")   
                    elif url == hn_rent_url:
                        driver.get("https://batdongsan.com.vn/nha-dat-cho-thue-ha-noi")
                    elif url == hcm_rent_url:
                        driver.get("https://batdongsan.com.vn/nha-dat-cho-thue-tp-hcm")
                                           
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
                    
                    
                    #Lấy dữ liệu lần đầu thì command điều kiện if này
                    if check_Database_Emty== False:
                        date_obj = datetime.strptime(day, "%d/%m/%Y").date()
                        if date_obj != datetime.now().date():
                            exit_while = True
                            break
                    
                    page_data.append([title, district, price, area, price_per_m2, bedrooms, toilets, day])
                    
                    last_successful_page = page_num 
                
                if exit_while:
                    driver.quit()

                # Lock to safely update the shared data structure
                with data_lock:
                    property_data.extend(page_data)
                    
                if page_num % 100 == 0 or page_num % 101 == 0 or page_num % 102 == 0 or page_num % 103 == 0:
                    driver.quit()
                    driver = uc.Chrome()
            
                
        except TimeoutException:
            print(f"Timeout occurred in thread {thread_id}")
            driver.quit()
            print("Restarting the driver in 5 seconds")
            time.sleep(5)
            driver = uc.Chrome()

            
        else:
            driver.quit()
            break    
def Write_data_Excel(df,file_name):
    df.to_excel(file_name, sheet_name='Sheet1',index=False)

def main(url):
    columns_headers = ['title', 'address', 'price', 'area', 'price_per_m2', 'bedrooms', 'toilets', 'date']

    engine=connect_My_SQL('root','1234','localhost','3307','BDS',url.split('/')[3].replace('-','_'))

    # Get total_pages in website
    url_new=url+'?sortValue=1'
    driver = uc.Chrome()
    driver.get(url_new)
    html = driver.page_source
    soup = BeautifulSoup(html, 'html.parser')
    total_pages = soup.find_all(class_="re__pagination-number")[-1].get_text()
    total_pages=total_pages.replace('.','')
    total_pages=int(total_pages)
    
  
    # driver.quit()

    property_data = []
    data_lock = threading.Lock()

    # Setup Chrome options for undetected_chromedriver

    num_threads = 2 # Number of chrome open

    threads = []
    for thread_id in range(1,num_threads + 1):
        driver = uc.Chrome()
        t = threading.Thread(target=process_pages, args=(thread_id, num_threads, total_pages, data_lock, property_data,driver,url))
        threads.append(t)
        t.start()

    for thread in threads:
        thread.join()

    #Write data to excel
    df=pd.DataFrame(property_data,columns=columns_headers)
    df.date=pd.to_datetime(df.date,format="%d/%m/%Y")
    file_name=url.split("/")[3] + ' ' + str(datetime.now().date()) +'.xlsx'
    Write_data_Excel(df,file_name)

    # Write data to mysql
    write_Data_To_Database(df,engine,url.split('/')[3].replace('-','_'))


    

if __name__ == "__main__":
    start = time.time()
    hcm_url = 'https://batdongsan.com.vn/nha-dat-ban-tp-hcm/p{}?sortValue=1'
    hn_url = 'https://batdongsan.com.vn/nha-dat-ban-ha-noi/p{}?sortValue=1'
    hcm_rent_url = 'https://batdongsan.com.vn/nha-dat-cho-thue-tp-hcm/p{}?sortValue=1'
    hn_rent_url = 'https://batdongsan.com.vn/nha-dat-cho-thue-ha-noi/p{}?sortValue=1'
    
    main(hcm_url)
    time.sleep(5)
    main(hn_url)
    time.sleep(5)
    main(hcm_rent_url)
    time.sleep(5)
    main(hn_rent_url)
    end = time.time()
    print(end-start)

