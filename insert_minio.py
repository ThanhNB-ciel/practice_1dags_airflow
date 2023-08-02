import pandas as pd
import boto3
from io import StringIO 
import csv
# from minio import Minio
import datetime
from io import BytesIO
from datetime import date, timedelta

def inser_minio():
    
    prev_date = date.today()- timedelta(1)
    file_path = f'/home/thanhnb/data_ck_{prev_date}_1.csv'
    # file_path = '/home/thanhnb/airflow/dags/ck.csv'

    with open(file_path, "r", encoding="utf-8") as f:
        data = f.read()
        

    client = boto3.client(
            "s3",
            endpoint_url='http://192.168.1.21:2345',
            aws_access_key_id='minioadmin',
            aws_secret_access_key='minioadmin',
        )
    client.put_object(
                Bucket='crawl',
                Key= f'crawl_ck/data_ck_{prev_date}_1.csv',
                Body=data,
        )   

inser_minio()