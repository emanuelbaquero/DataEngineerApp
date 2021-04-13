import json, subprocess, random, time,os,io
from flask import Flask, request
from flask_restplus import Api, Resource, fields, reqparse, inputs
from flask import Flask, render_template
from flask import make_response, request, Response
from flask import jsonify
from json import JSONEncoder
import pandas as pd
from datetime import datetime
from werkzeug.middleware.proxy_fix import ProxyFix
import subprocess
import pandasql as ps
import boto3
import pymongo
from pymongo import MongoClient

dir_local_img_default = '/home/flask-restfull/'

app = Flask(__name__, template_folder='templates')
api = Api(app, version='1.0', title='API exercise-data-engineer',
    description='API exercise-data-engineer',
)


def getDataFrame(table):

    client = MongoClient("mongodb+srv://mongo:mongo@cluster0.yjbeg.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
    db = client.get_database('test')

    list_serie = []

    if (table == 'What_Are_The_Two_Drier_City_Region'):
        data = db.What_Are_The_Two_Drier_City_Region

        for i in data.find():
            list_serie.append(pd.Series(i))

        df_mongo = pd.DataFrame({'region': list_serie[0],
                                 'humidity_mean': list_serie[1]
                                 })

    if (table == 'What_Is_The_Warmest_City_Region'):
        data = db.What_Is_The_Warmest_City_Region
        for i in data.find():
            list_serie.append(pd.Series(i))

        df_mongo = pd.DataFrame({'region': list_serie[0],
                                 'the_temp_mean': list_serie[1]
                                 })

    if (table == 'What_Is_The_Hottes_Day_For_Each_Month'):
        data = db.What_Is_The_Hottes_Day_For_Each_Month
        for i in data.find():
            list_serie.append(pd.Series(i))

        df_mongo = pd.DataFrame({'region': list_serie[0],
                                 'mes': list_serie[1],
                                 'dia': list_serie[2],
                                 'the_temp': list_serie[3]
                                 })
    df_mongo = df_mongo.iloc[1:]

    return df_mongo



@api.route('/What_Is_The_Warmest_City_Region')
class What_Is_The_Warmest_City_Region(Resource):
    def get(self):

        time_start = datetime.now() 

        df = getDataFrame('What_Is_The_Warmest_City_Region')


        region = df.head(1)['region'].values[0]
        temperature = df.head(1)['the_temp_mean'].values[0]

        time_end  = datetime.now()

        return {'time': str(time_end-time_start),'region':region,'mean_temperature':temperature}




@api.route('/What_Are_The_Two_Drier_City_Region')
class What_Are_The_Two_Drier_City_Region(Resource):
    def get(self):

        time_start = datetime.now() 

        df = getDataFrame('What_Are_The_Two_Drier_City_Region')


        region_baja_humedad_1 = df.head(2).head(1)['region'].values[0]
        humedad_1 = df.head(1)['humidity_mean'].values[0]

        region_baja_humedad_2 = df.head(2).tail(1)['region'].values[0]
        humedad_2 = df.head(2).tail(1)['humidity_mean'].values[0]

        time_end  = datetime.now()

        return {'time': str(time_end-time_start),'primera_region':{region_baja_humedad_1:{'mean_humidity':humedad_1}},'segunda_region':{region_baja_humedad_2:{'mean_humidity':humedad_2}}}



@api.route('/What_Is_The_Hottes_Day_For_Each_Month_For_Each_City_Region')
class What_Is_The_Hottes_Day_For_Each_Month_For_Each_City_Region(Resource):
    def get(self):

        time_start = datetime.now() 

        df = getDataFrame('What_Is_The_Hottes_Day_For_Each_Month')

        time_end  = datetime.now()


        return {   'time': str(time_end-time_start),


                        'primera_region':
                                            {
                                                'mes_1':{
                                                                'region':df[(df.region=='Buenos Aires') & (df.mes==1)]['region'][0],
                                                                'dia':str(df[(df.region=='Buenos Aires') & (df.mes==1)]['dia'][0]),
                                                                'temperature':str(df[(df.region=='Buenos Aires') & (df.mes==1)]['the_temp'][0])
                                                        },
                                                'mes_2':{
                                                        'region':df[(df.region=='Buenos Aires') & (df.mes==2)]['region'][0],
                                                        'dia':str(df[(df.region=='Buenos Aires') & (df.mes==2)]['dia'][0]),
                                                        'temperature':str(df[(df.region=='Buenos Aires') & (df.mes==2)]['the_temp'][0])
                                                        },
                                                'mes_3':{
                                                        'region':df[(df.region=='Buenos Aires') & (df.mes==3)]['region'][0],
                                                        'dia':str(df[(df.region=='Buenos Aires') & (df.mes==3)]['dia'][0]),
                                                        'temperature':str(df[(df.region=='Buenos Aires') & (df.mes==3)]['the_temp'][0])
                                                        }


                                            },


                        'segunda_region':
                                            {
                                                'mes_1':{
                                                                'region':df[(df.region=='Santiago') & (df.mes==1)]['region'][0],
                                                                'dia':str(df[(df.region=='Santiago') & (df.mes==1)]['dia'][0]),
                                                                'temperature':str(df[(df.region=='Santiago') & (df.mes==1)]['the_temp'][0])
                                                        },
                                                'mes_2':{
                                                                'region': df[(df.region == 'Santiago') & (df.mes == 2)]['region'][0],
                                                                'dia': str(df[(df.region == 'Santiago') & (df.mes == 2)]['dia'][0]),
                                                                'temperature': str(df[(df.region == 'Santiago') & (df.mes == 2)]['the_temp'][0])
                                                        },
                                                'mes_3':{
                                                                'region': df[(df.region == 'Santiago') & (df.mes == 3)]['region'][0],
                                                                'dia': str(df[(df.region == 'Santiago') & (df.mes == 3)]['dia'][0]),
                                                                'temperature': str(df[(df.region == 'Santiago') & (df.mes == 3)]['the_temp'][0])
                                                        }


                                            },



                        'tercera_region':
                                            {
                                                'mes_1':{
                                                                'region':df[(df.region=='Brasilia') & (df.mes==1)]['region'][0],
                                                                'dia':str(df[(df.region=='Brasilia') & (df.mes==1)]['dia'][0]),
                                                                'temperature':str(df[(df.region=='Brasilia') & (df.mes==1)]['the_temp'][0])
                                                        },
                                                'mes_2':{
                                                                'region': df[(df.region == 'Brasilia') & (df.mes == 2)]['region'][0],
                                                                'dia': str(df[(df.region == 'Brasilia') & (df.mes == 2)]['dia'][0]),
                                                                'temperature': str(df[(df.region == 'Brasilia') & (df.mes == 2)]['the_temp'][0])
                                                        },
                                                'mes_3':{
                                                                'region': df[(df.region == 'Brasilia') & (df.mes == 3)]['region'][0],
                                                                'dia': str(df[(df.region == 'Brasilia') & (df.mes == 3)]['dia'][0]),
                                                                'temperature': str(df[(df.region == 'Brasilia') & (df.mes == 3)]['the_temp'][0])
                                                        }


                                            }



                    }


  

if __name__ == '__main__':
    app.wsgi_app = ProxyFix(app.wsgi_app)
    app.debug = True
    app.run(host='0.0.0.0', port=5000)



