import requests
from bs4 import BeautifulSoup as bs
import pandas as pd 
from dateutil import parser
from helpers.general_tools import flatten,get_text_news,get_title_news

def transform(lista,links) :
    df = pd.DataFrame.from_records(lista)
    df.columns = ["titulos","textos","data_publicacao"]
    df["fontes"] = "The Hackers News"
    df["tema_principal"] = "CyberSecurity"
    df["links"] = links
    return df 

def get_links_hackernews(url) :
    r = bs(requests.get(url).text,"html.parser")
    links_status= []
    for link in r.find_all("a") :
        file_link = link.get("href") 
        if 'https://thehackernews.com' in file_link :
            if  'https://thehackernews.com/search?' in file_link :
                next_link = file_link
            else :
                links_status.append(file_link)
    return links_status,next_link

def get_all_links_hackerrank(number_of_pages) :

    url ="https://thehackernews.com/"
    all_links = []
    for number in range(number_of_pages) :
        links, url = get_links_hackernews(url)
        all_links.append(links)
    list_corrida = flatten(all_links)
    
    return list_corrida




def get_date_author_news_hackerrank(response) :
    count = 0
    date = None
    for link in response.find_all(class_="author") :
        if count == 0 :
            date=(link.text) 
            date=parser.parse(date).date()

        count += 1
    return date


def get_data_from_hackernews(url) :
    response = bs(requests.get(url).text,"html.parser")
    texto = get_text_news(response)
    titulo =get_title_news(response) 
    date  = get_date_author_news_hackerrank(response) 
    return titulo,texto,date


def data_to_dataframe_hackerank(link_urls) :
    
    list_of_situations = [get_data_from_hackernews(url) for url in link_urls]
    return list_of_situations
    
