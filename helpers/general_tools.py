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

def remove_garbage(bucket_gcs,filename) :
    bucket = client.get_bucket(bucket_gcs)
    # list all objects in the directory
    directory = "trusted-inteligencia_mercado"
    blobs = bucket.list_blobs(prefix=directory)
            
    tem_arquivo = "Não achou nenhum arquivo com esse nome"
    count_geral = 0
    count_deletados = 0
    for blob in blobs :
        count_geral += 1
        if filename in blob.name :
            print(blob.name)
            blob.delete()
            tem_arquivo = "Achou arquivo com esse nome"
            count_deletados += 1

    return tem_arquivo, count_deletados,count_geral    
        
def clean_dataframe_titulos(df) :

    PALAVRAS_GERAIS = ["inscrição","palestra","webninário","inscrever","Forum",
            "livro","vagas","mortes","cursos","morte","SEGS","covid","COVID","Galaxy",
            "Haddad","Veja","veja","Bolsonaro","MBA","dicas","dica","Ciência Fundamental",
            "pesquisa","diversidade","IR","feriado","IBGE","Meet the Expert"
             "saque","confira","evento","documentário"]


    df['flagCol'] = np.where(df.titulos.str.contains('|'.join(PALAVRAS_GERAIS)),1,0)
    mask_geral = df['flagCol'] == 1
    df = df.drop(columns = "flagCol")
    df =df[~mask_geral]
    
    mask_eleicoes = df["titulos"].str.contains("%") & df["titulos"].str.contains("Big Data")
    df =df[~mask_eleicoes]    
    return df 