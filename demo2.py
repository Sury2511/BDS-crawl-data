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
from selenium import webdriver
from cassandra.cluster import Cluster
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,DateType
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
import pandas as pd 
import os
from pyspark.sql.functions import expr,monotonically_increasing_id
from pyspark.sql.window import Window
from pyspark.sql.functions import rank
from pyspark.sql.functions import lit
import pyspark.sql.functions as sf

# creat spark session
spark = SparkSession.builder.config("spark.driver.memory", "8g").config('spark.jars.packages','com.datastax.spark:spark-cassandra-connector_2.12:3.1.0').getOrCreate()

def connect_My_SQL(user,psw,host,port,database,table_name):
    engine = create_engine('mysql+mysqlconnector://{}:{}@{}:{}/{}'.format(user,psw,host,port,database))
    engine.connect()
    check_day=pd.read_sql("SELECT MAX(date) as MAX_DATE FROM {}".format(table_name),con=engine)
    check_day=check_day["MAX_DATE"][0]
    if check_day == None:
        check_Database_Emty=True
    else:
        check_Database_Emty=False
    return engine , check_Database_Emty


def connect_Cassandra(host,port1,keyspace1,table_name):
    cluster=Cluster([host],port=port1)
    session=cluster.connect(keyspace1)
    rows=session.execute('SELECT max(date) as MAX_Date FROM {};'.format(table_name))
    check_day=0
    for i in rows:
        check_day=i.max_date
    if check_day == None:
        check_Database_Emty=True
    else:
        check_Database_Emty=False
    return  check_Database_Emty

def wirte_Data_To_Cassandra(df,table_name,keyspace_name):
    df.write.format("org.apache.spark.sql.cassandra").options(table=table_name, keyspace=keyspace_name).mode("append").save()


def write_Data_To_Database(df,engine,table_name):
    df.to_sql(name=table_name,con=engine,if_exists='append',index=False)


def read_Data_From_Cassandra(df,table_name,keyspace_name,):
    data = df.read.format("org.apache.spark.sql.cassandra").options(table=table_name, keyspace=keyspace_name).load()


def process_pages(thread_id, num_threads, total_pages, data_lock, property_data,driver,url,check_database):
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
                    if check_database== False:
                        date_obj = datetime.strptime(day, "%d/%m/%Y").date()
                        Test_address=address.split(',')[-1].strip()
                        if date_obj != datetime.now().date() and (Test_address == 'Hà Nội' or Test_address == 'Hồ Chí Minh'):
                            exit_while = True
                            break
                    
                    page_data.append([title, district, price, area, price_per_m2, bedrooms, toilets, day])
                    
                    last_successful_page = page_num 
                


                # Lock to safely update the shared data structure
                with data_lock:
                    property_data.extend(page_data)

                if exit_while:
                    driver.quit()
                    break
                    
                # if page_num % 100 == 0 or page_num % 101 == 0 or page_num % 102 == 0 or page_num % 103 == 0:
                #     driver.quit()
                #     opts = uc.ChromeOptions()
                #     opts.add_argument("--no-sandbox")
                #     opts.add_argument("--disable-dev-shm-usage")
                #     opts.add_argument("--disable-renderer-backgrounding")
                #     opts.add_argument("--disable-background-timer-throttling")
                #     opts.add_argument("--disable-backgrounding-occluded-windows")
                #     opts.add_argument("--disable-client-side-phishing-detection")
                #     opts.add_argument("--disable-crash-reporter")
                #     opts.add_argument("--disable-oopr-debug-crash-dump")
                #     opts.add_argument("--no-crash-upload")
                #     opts.add_argument("--disable-gpu")
                #     opts.add_argument("--disable-extensions")
                #     opts.add_argument("--disable-low-res-tiling")
                #     opts.add_argument("--log-level=3")
                #     opts.add_argument("--silent")
                #     driver = uc.Chrome(options=opts)
            # if exit_while:
            #     break      
            
                
        except TimeoutException:
            print(f"Timeout occurred in thread {thread_id}")
            driver.quit()
            print("Restarting the driver in 6 seconds")
            time.sleep(5)
            opts = uc.ChromeOptions()
            opts.add_argument("--no-sandbox")
            opts.add_argument("--disable-dev-shm-usage")
            opts.add_argument("--disable-renderer-backgrounding")
            opts.add_argument("--disable-background-timer-throttling")
            opts.add_argument("--disable-backgrounding-occluded-windows")
            opts.add_argument("--disable-client-side-phishing-detection")
            opts.add_argument("--disable-crash-reporter")
            opts.add_argument("--disable-oopr-debug-crash-dump")
            opts.add_argument("--no-crash-upload")
            opts.add_argument("--disable-gpu")
            opts.add_argument("--disable-extensions")
            opts.add_argument("--disable-low-res-tiling")
            opts.add_argument("--log-level=3")
            opts.add_argument("--silent")
            driver = uc.Chrome(options=opts)
          

            
        else:
            driver.quit()
            break    

def Write_data_csv(df,file_name):
    df.repartition(1).write.csv(file_name,header=True)

def main(url):
    # Chrome options
    opts = uc.ChromeOptions()
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-renderer-backgrounding")
    opts.add_argument("--disable-background-timer-throttling")
    opts.add_argument("--disable-backgrounding-occluded-windows")
    opts.add_argument("--disable-client-side-phishing-detection")
    opts.add_argument("--disable-crash-reporter")
    opts.add_argument("--disable-oopr-debug-crash-dump")
    opts.add_argument("--no-crash-upload")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-extensions") ## vo hieu hoa cac extendtion
    opts.add_argument("--disable-low-res-tiling")
    opts.add_argument("--log-level=3")
    opts.add_argument("--silent")


    columns_headers = ['title', 'address', 'price', 'area', 'price_per_m2', 'bedrooms', 'toilets', 'author', 'date']
    ## connect mysql
    # engine,check_database=connect_My_SQL('root','1234','localhost','3307','BDS',url.split('/')[3].replace('-','_'))
    check_cassandra=connect_Cassandra('localhost',9042,'raw_bds',url.split('/')[3].replace('-','_'))
    

    #Get total_pages in website
    url_new=url.replace('/p{}','')
    # print(url_new)
    driver = uc.Chrome(options=opts)
    driver.get(url_new)
    html = driver.page_source
    soup = BeautifulSoup(html, 'html.parser')
    total_pages = soup.find_all(class_="re__pagination-number")[-1].get_text()
    total_pages=total_pages.replace('.','')
    total_pages=int(total_pages)
    print(total_pages)
    driver.quit()
    

    property_data = []
    data_lock = threading.Lock()

    # Setup Chrome options for undetected_chromedriver

    num_threads = 2 # Number of chrome open

    threads = []
    for thread_id in range(1,num_threads + 1):
        opts = uc.ChromeOptions()
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-renderer-backgrounding")
        opts.add_argument("--disable-background-timer-throttling")
        opts.add_argument("--disable-backgrounding-occluded-windows")
        opts.add_argument("--disable-client-side-phishing-detection")
        opts.add_argument("--disable-crash-reporter")
        opts.add_argument("--disable-oopr-debug-crash-dump")
        opts.add_argument("--no-crash-upload")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--disable-extensions")
        opts.add_argument("--disable-low-res-tiling")
        opts.add_argument("--silent")
        driver = uc.Chrome(options=opts)
        t = threading.Thread(target=process_pages, args=(thread_id, num_threads, total_pages, data_lock, property_data,driver,url,check_cassandra))
        threads.append(t)
        t.start()

    for thread in threads:
        thread.join()

    #Write data to excel
    # df=pd.DataFrame(property_data,columns=columns_headers)
    df=spark.createDataFrame(property_data,schema='title string, address string, price string, area string, price_per_m2 string, bedrooms string, toilets string, date string')
    df = df.withColumn("date", to_date(df["date"], "dd/MM/yyyy").cast(DateType()))
    df=df.withColumn('id',sf.expr('uuid()'))
    # df.date=pd.to_datetime(df.date,format="%d/%m/%Y")
    file_name='E:/Project_UD_Du_lieu_Thong_Minh/Crawl data/'+ url.split("/")[3] + ' ' + str(datetime.now().date())
    Write_data_csv(df,file_name)

    # # # Write data to mysql
    # write_Data_To_Database(df,engine,url.split('/')[3].replace('-','_'))
    wirte_Data_To_Cassandra(df,url.split('/')[3].replace('-','_'),'raw_bds')


    

if __name__ == "__main__":
 
    start = time.time()
    hcm_url = 'https://batdongsan.com.vn/nha-dat-ban-tp-hcm/p{}?sortValue=1'
    hn_url = 'https://batdongsan.com.vn/nha-dat-ban-ha-noi/p{}?sortValue=1'
    hcm_rent_url = 'https://batdongsan.com.vn/nha-dat-cho-thue-tp-hcm/p{}?sortValue=1'
    hn_rent_url = 'https://batdongsan.com.vn/nha-dat-cho-thue-ha-noi/p{}?sortValue=1'
    
    main(hcm_url)
    print('Sucessfull nha-dat-ban-tp-hcm ')
    time.sleep(5)
    main(hn_url)
    print('Sucessfull nha-dat-ban-ha-noi ')
    time.sleep(5)
    main(hcm_rent_url)
    print('Sucessfull nha-dat-cho-thue-tp-hcm ')
    time.sleep(5)
    main(hn_rent_url)
    print('Sucessfull ha-dat-cho-thue-ha-noi ')
    end = time.time()
    print(end-start)

