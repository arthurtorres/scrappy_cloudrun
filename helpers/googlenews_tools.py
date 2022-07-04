import requests
from bs4 import BeautifulSoup as bs
import pandas as pd 
from urllib.parse import urljoin
from datetime import date 

def convert_dates(dates) :
    dates =str(dates)
    date = dates.split('datetime="')[-1].split('"')[0][:10]
    return date 


def clean_df(df) :

    df["data_publicacao"] = df["data_publicacao"].apply(lambda x : convert_dates(x))
    filter =(df["data_publicacao"] !="None") & (df["data_publicacao"] >="2022-01-01")
    df = df[filter]
    df = df[df["textos"] != "Link não acessivel/Sem Acesso"]
    return df
    
def get_dataframe(pesquisa,tempo) :
    url = f'https://news.google.com/search?q={pesquisa}when:{tempo}&hl=pt-BR&gl=BR&ceid=BR:pt-419'
    CLASSE_ARTIGOS = "MQsxIb xTewfe R7GTQ keNKEd j7vNaf Cc0Z5d EjqUne"
    CLASSE_FONTES =  "wEwyrc AVN2gc uQIVzc Sksgp"
    CLASSE_DATAS = "WW6dff uQIVzc Sksgp"
    CLASSE_LINK = "DY5T1d"
    html = bs(requests.get(url).text,"html.parser")
    articles = html.find_all(class_ =CLASSE_ARTIGOS)
    dados = [get_info(article,CLASSE_LINK,CLASSE_DATAS,CLASSE_FONTES) for article in articles ]
    df = pd.DataFrame.from_records(dados)
    df.columns = ["links","titulos","fontes","data_publicacao","textos"]
    return df 



def get_info(article,class_link,class_data,class_fonte) :
    URL_BASE = "https://news.google.com/"
    link = article.find(class_ =class_link)
    link_completo =urljoin(URL_BASE,link.get("href"))
    titulo = link.text
    fonte = article.find(class_ = class_fonte).text
    date = article.find(class_ = class_data)
    texto = get_texto_from_news(link_completo)
    return link_completo,titulo,fonte,date,texto

def get_texto_from_news(link_completo) :
    try :
        html = bs(requests.get(link_completo,timeout= 10).content,"html.parser")
        texto = (get_text_news(html))
    except :        
        texto = "Link não acessivel/Sem Acesso"

    return texto

def get_text_news(response):
    texto = ""
    for link in response.find_all("p") :
        texto = texto +str(link) + "\n"
    cleantext = bs(texto, "lxml").text
    return cleantext

