import numpy as np
from selenium import webdriver
import time
from time import sleep
import random
from selenium.common.exceptions import NoSuchElementException, ElementNotInteractableException
from selenium.webdriver.common.by import By
import pandas as pd
import os
from bs4 import BeautifulSoup
import requests
import pandas as pd
import requests
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from datetime import date, timedelta
# from clickhouse_driver import Client
from selenium.webdriver.chrome.options import Options

def crawl_ck():
    
    current_day = (date.today()-timedelta(1)).weekday()
    if current_day == 5 or current_day == 6: 
        return
    else:
        options = Options()
        options.add_argument('--headless')
        # chrome_options.add_argument('--no-sandbox')
        # chrome_options.add_argument('--disable-dev-shm-usage')
        options.add_argument("--start-maximized") #open Browser in maximized mode
        options.add_argument("--no-sandbox") #bypass OS security model
        options.add_argument("--disable-dev-shm-usage") #overcome limited resource problems
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        driver = webdriver.Chrome(executable_path="chromedriver", chrome_options=options)
        driver.get('http://s.cafef.vn/screener.aspx')
        table_id = driver.find_element(By.XPATH,'//*[@id="myTable"]/tbody')
        
        dfx=[]
        rows = table_id.find_elements(By.TAG_NAME, 'tr')
        
        for row in rows[:10]:
            col = [col.text for col in row.find_elements(By.TAG_NAME, "td")]
            df = pd.DataFrame([col],columns=['stt','ten_cong_ty','ma_co_phieu','san_chung_khoan','thay_doi_5_phien_truoc','von_hoa_thi_truong','klgd','eps','p_e','he_so_beta','gia'])
            dfx.append(df)
            df = pd.concat(dfx, ignore_index=True)
            df = df.sort_values(by='gia',ascending = False)
            df['date'] = [date.today()- timedelta(1)] * len(df.index)
            df = df[['date','ten_cong_ty','ma_co_phieu','san_chung_khoan','thay_doi_5_phien_truoc','von_hoa_thi_truong','klgd','eps','p_e','he_so_beta','gia']]
            prev_date = date.today()- timedelta(1)
            # df.to_csv(f'data_ck_{prev_date}.csv', index=False)
        # return df


        print(df)
        time.sleep(5)
        driver.close()
crawl_ck()
