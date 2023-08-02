import pandas as pd
from clickhouse_driver import Client
import numpy as np
from selenium import webdriver
import time
from time import sleep
from datetime import date, timedelta
# from clickhouse_driver import Client\


def insert_data():
    prev_date = date.today()- timedelta(1)
    # data = crawl_ck(driver)
    clickhouse_info = {
        "host": "192.168.1.52",
        "user": "default",
        "password": "",
        'port' : "19000"
    }
    client = Client(host=clickhouse_info['host'], port=clickhouse_info['port'], user=clickhouse_info['user'],
                    password=clickhouse_info['password'], settings={
        'use_numpy': True}
    )

    # client = Client(host ='localhost', port=9002, user="default", settings ={'use_numpy':True})
    # client.insert_dataframe("insert into thanhnb.chung_khoan values", data)

insert_data()