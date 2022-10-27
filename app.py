
from pipes import Template

from numpy import number
from flask import Flask, jsonify, request
from flask_restful import Api, Resource
from helpers.general_tools import dtProcessamento_option,df_to_gcs
from helpers.hackernews_tools import get_all_links_hackerrank,data_to_dataframe_hackerank,transform
from helpers.prensa_tools import create_dataframe_prensa
from datetime import datetime
app = Flask(__name__)
api = Api(app)

"""
RESOURCES
"""


class getTheHackerNews(Resource):
    def post(self):
        print("HERE THN")
        try :

            # Get posted data from request
            data = request.get_json()
            pages = int(data["numberofPages"])
            print("Comecou")
            data = datetime.date(datetime.today())
            links_hackerrank = get_all_links_hackerrank(pages)
            links_hackerrank_no_duplicates = list(set(links_hackerrank))
            list_infos = data_to_dataframe_hackerank(links_hackerrank_no_duplicates) 
            df = transform(list_infos,links_hackerrank_no_duplicates)
            df =dtProcessamento_option(df)
            df_to_gcs("portifolio-arthur",f"std-raw/scrappy/HackerNews/data_collect_Security_{data}.parquet",df)
            result = "Sucesso"
        except :
            result = "Fracasso"

        return jsonify({"result": result})

class getPrensa(Resource) :
    def post(self) :
            try : 
                data = request.get_json()
                tema = data["tema"]
                pesquisa= data["pesquisa"]            
                number_of_pages = data["numero_de_paginas"]

                data = datetime.date(datetime.today())
                df =create_dataframe_prensa(pesquisa,number_of_pages)
                df["tema_principal"] = tema

                df =dtProcessamento_option(df)
                df_to_gcs("portifolio-arthur",f"std-raw/scrappy/P-{tema}/{pesquisa}/data-collect-{number_of_pages}-{data}.parquet",df)
                result = "Sucesso"
                quantidade = df.shape[0]
            except :
                result = "Fracasso"
                quantidade = -1

            return jsonify({"result": result ,
                        "Quantidade" : quantidade})

api.add_resource(getTheHackerNews, '/postTheHackerNews')
api.add_resource(getPrensa, '/getPrensa')

if __name__ == "__main__":
   app.run(host='0.0.0.0', port='8080', debug=True)
   