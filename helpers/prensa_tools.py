from datetime import datetime,date
import requests
from bs4 import BeautifulSoup as bs
import pandas as pd 
#from helpers.general_tools import flatten 

def flatten(t):
    return [item for sublist in t for item in sublist]
def convert_to_date_with_year(lista) :

    year = date.today().year
    last_month = date.today().month
    datas_completas =[]
    for value in lista :
        month = value[-3:]
        if last_month == "Jan" and month == "Dec" :
            print("Entrei Aqui")
            year -=1
            
        last_month = month[:]
        data = f"{value}-{year}".replace(" ","-")
        data = datetime.strptime(data, "%d-%b-%Y").date()
        datas_completas.append(data)
    return datas_completas
    
def convert_to_date(lista) :

    bad_months = [("Mai","May"),("Abr","Apr"),("Fev","Feb"),("Dez","Dec"),("Out","Oct")]
    for monthes in bad_months :
        lista = [lis.replace(monthes[0],monthes[1]) for lis in lista]
    return lista



def get_links_titles_from_response(response) :
    URL_BASE = "https://prensa.li"

    major_obj= response.find(class_ ="col-md-8")
    
    minor_obj_title_links= major_obj.find_all(class_ ="entry-title mb-3")
    minor_obj_date= major_obj.find_all(class_ ="entry-meta align-items-center")

    links_total = []
    titulos = []
    dates_total = []
    for links in  (minor_obj_title_links) :
        for link in links.find_all('a', href=True) :
              titulos.append(link.text)
              links_total.append(f'{URL_BASE}{link["href"]}')
    for date in minor_obj_date :
                
            dates_total.append(date.find("span").text)

    return links_total,titulos,dates_total
 
def get_info(tema,number_of_pages) :
    url = f"https://prensa.li/t/{tema}/"
    links_total = []
    titulos_total = []
    dates_total = []
    for number in range(1,number_of_pages+1) :
       
        url_page= f'{url}?page={number}'
        html = bs(requests.get(url_page).text,"html.parser")
        link,titulo,date = get_links_titles_from_response(html)
        links_total.append(link)
        titulos_total.append(titulo)
        dates_total.append(date)
    return links_total,titulos_total,dates_total


def get_text_from_link(link) :

    response= bs(requests.get(link).text,"html.parser")
    response = response.find(class_="non-paywall")
    cleantext = "Não achou o texto por algum motivo aleatorio"
    if response :
        texto = ""
        for link in response.find_all("p") :
            texto = texto +str(link) + "\n"
            cleantext = bs(texto, "lxml").text
    else :
        cleantext = "Esse texto é um evento"
    return cleantext

def create_dataframe_prensa(pesquisa,number_of_pages) :
    
    links,titulos,datas = get_info(pesquisa,number_of_pages) 
    
    datas=flatten(datas)
    links = flatten(links)
    titulos = flatten(titulos)
    datas = convert_to_date(datas)
    datas = convert_to_date_with_year(datas)
    textos =[get_text_from_link(link) for link in links]


    df = pd.DataFrame.from_dict({"links" : links,
                              "titulos" : titulos ,
                          "data_publicacao" : datas,
                          "textos" : textos} )
    filter = ~((df["textos"] == "Esse texto é um evento") | (df["textos"] == "Não achou o texto por algum motivo aleatorio"))                                                      
                                                                    
    df = df[filter]
    df["fontes"] = "Prensa"

    return df
