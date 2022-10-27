from google.cloud import storage # Google Cloud Storage
from datetime import datetime 
client = storage.Client()
import io 
import pandas as pd
from bs4 import BeautifulSoup as bs 
import numpy as np

def get_title_news(response) :
    for link in response.find_all('h1') :
        titulo =link.text
        return titulo
def get_text_news(response):
    texto = ""
    for link in response.find_all("p") :
        texto = texto +str(link) + "\n"
    cleantext = bs(texto, "lxml").text
    return cleantext

def flatten(t):
    return [item for sublist in t for item in sublist]

def dtProcessamento_option(df,how="by_today_date",filename = None,date=None):
    
    if how == "by_filename" :
        datas = filename.split('_')[-1].split('.xlsx')[0]
        data = f'{datas[0:4]}-{datas[4:6]}-{datas[6:]}'
    elif how == "by_fix_date" :
        data = date
    elif how == "by_today_date" :
        data = datetime.date(datetime.today())
    df["dtProcessamento"] = data
    df['dtProcessamento'] = pd.to_datetime(df["dtProcessamento"]).dt.date
    return df

def df_to_gcs(bucket_gcs,filepath,dataframe):
    io_file = io.BytesIO()
    dataframe.to_parquet(io_file, index = False)     
    buckets = client.get_bucket(bucket_gcs)
    blob = buckets.blob(filepath)
    blob.upload_from_string(io_file.getvalue(), content_type='application/octet-stream')

        