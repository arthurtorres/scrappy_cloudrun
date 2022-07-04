import requests 
from bs4 import BeautifulSoup as bs
import pandas as pd
from datetime import datetime 
from helpers.general_tools import flatten,get_text_news,get_title_news


def get_links_inforchannel(tema,numberofPages) :
    url = f"https://inforchannel.com.br/?post_type=post&s={tema}"
    links = []
    # pegar os links da URL original
    links.append(get_links(url))
    #pegar os links das URL com paginação, dado um numero de páginas
    for number in range(2,numberofPages+1) :
        url =f"https://inforchannel.com.br/page/{number}/?post_type=post&s={tema}" 
        links.append(get_links(url))
        print(number)

    links = flatten(links)
    return links 



def get_links(url) :
    r = bs(requests.get(url).text,"html.parser")
    rows = r.find_all(class_="row")
    list_links = []   
    for row in rows:
        container_tags_a = row.find_all(class_="col-lg-4")
        for tag_img in container_tags_a:
                b = tag_img.find_all(class_ = "img-fluid thumbnail-search")

                if b != []:

                    a = tag_img.find("a")
                    link = a.get("href")
                    if link not in list_links :
                        list_links.append(link)
    return list_links








def create_dataframe(tag,numberofPages) :
    linha = []
    links = get_links_inforchannel(tag,numberofPages) 
    for link in links :
        r = bs(requests.get(link).text,"html.parser")
        j= get_title_news(r)
        k  = get_text_news(r) 
        text_date= link[28:38]
        date = datetime.strptime(text_date,"%Y/%m/%d").date()
        linha.append([link,j,k,date])

    df =pd.DataFrame.from_records(linha)
    df.columns = ["links","titulos","textos","data_publicacao"]
    df["fontes"] = "InforChannel"
    return df 