
from pipes import Template

from numpy import number
from flask import Flask, jsonify, request
from flask_restful import Api, Resource
from helpers.general_tools import dtProcessamento_option,df_to_gcs,remove_garbage,clean_dataframe_titulos
from helpers.hackernews_tools import get_all_links_hackerrank,data_to_dataframe_hackerank,transform
from helpers.googlenews_tools import get_dataframe,clean_df
from helpers.inforchannel_tools import create_dataframe
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
class getGoogleNews(Resource) :
    def post(self) :

        try :
            data = request.get_json()
            tema = data["tema"]
            pesquisa = data["pesquisa"]
            tempo = data["tempo"]
            data = datetime.date(datetime.today())
            df =get_dataframe(pesquisa,tempo)
            df = clean_df(df)
            tamanho_original = df.shape[0]
            df = clean_dataframe_titulos(df)
            tamanho_final = df.shape[0]
            qtd_removidas = tamanho_original-tamanho_final
            df["tema_principal"] = tema
            df =dtProcessamento_option(df)
            df_to_gcs("portifolio-arthur",f"std-raw/scrappy/GN-{pesquisa}/{tempo}/data-collect-{tema}-{data}.parquet",df)
            result = "Sucesso"
            quantidade = df.shape[0]
        except :
            result = "Fracasso"
            quantidade = -1
            qtd_removidas = -1
        return jsonify({"result": result ,
                        "Quantidade" : quantidade,
                        "qtd_removidas" : qtd_removidas})


class removeGarbage(Resource) :
    def post(self) :
            data = request.get_json()
            titulo = data["titulo"]
            result,count_del,count_geral = remove_garbage("4sys-data",titulo)
            if result == "Achou arquivo com esse nome" :
                count = count_del
                
            else :
                count = count_geral

            return jsonify({"result": result,
                            "Quantidade" : count} )



api.add_resource(getTheHackerNews, '/postTheHackerNews')
api.add_resource(getGoogleNews, '/getGoogleNews')
api.add_resource(removeGarbage, '/garbageRemove')

if __name__ == "__main__":
   app.run(host='0.0.0.0', port='8080', debug=True)
   