import json, subprocess, random, time,os,io

from flask import Flask, request
from flask_restplus import Api, Resource, fields, reqparse, inputs
from flask import Flask, render_template
from flask import make_response, request, Response
from flask import jsonify
from json import JSONEncoder
from bson import ObjectId

from pymongo import MongoClient
import pandas as pd
from datetime import datetime
from werkzeug.middleware.proxy_fix import ProxyFix



##CONECTION
#USERNAME_MONGO = 'mongo'
#PASSWORD_MONGO = 'mongo'
#HOSTNAME_MONGO = '192.168.0.94'
#PORT_MONGO = '27017'
#DATABASE_MONGO = 'test'
#TABLE_MONGO = 'st_bsas'


#MONGO_URI = 'mongodb://'+USERNAME_MONGO+':'+PASSWORD_MONGO+'@'+HOSTNAME_MONGO+':'+PORT_MONGO
## ME CONECTO AL CLIENTE DE MONGODB 
#client = MongoClient(MONGO_URI)
#db = client['test']['data_st_regiones']
client = MongoClient("mongodb+srv://mongo:mongo@cluster0.yjbeg.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
db = client.get_database('test')
data = db.data_st_regiones 




dir_local_img_default = '/home/flask-restfull/'

app = Flask(__name__, template_folder='templates')
api = Api(app, version='1.0', title='API exercise-data-engineer',
    description='API exercise-data-engineer',
)


def getDataFrame():
    list_serie=[]
    for i in data.find():
        list_serie.append(pd.Series(i))

    df_mongo = pd.DataFrame({'id':list_serie[0],
                            'weather_state_name':list_serie[1],
                            'weather_state_abbr':list_serie[2],
                            'wind_direction_compass':list_serie[3], 
                            'created':list_serie[4], 
                            'applicable_date':list_serie[5], 
                            'min_temp':list_serie[6],
                            'max_temp':list_serie[7], 
                            'the_temp':list_serie[8], 
                            'wind_speed':list_serie[9], 
                            'wind_direction':list_serie[10], 
                            'air_pressure':list_serie[11],
                            'humidity':list_serie[12], 
                            'visibility':list_serie[13], 
                            'predictability':list_serie[14],
                             'region':list_serie[15]
                            })
    df_mongo = df_mongo.iloc[1:,:]

    return df_mongo










    


@api.route('/shape')
class Shape(Resource):
    def get(self):

        time_start = datetime.now() 

        df = getDataFrame()

        dimensiones = df.shape




        time_end  = datetime.now()

        return {'time': str(time_end-time_start),'shape':dimensiones}




@api.route('/What_Is_The_Warmest_City_Region')
class What_Is_The_Warmest_City_Region(Resource):
    def get(self):

        time_start = datetime.now() 

        df = getDataFrame()

        df.the_temp = df.the_temp.astype(float)
        region = df.groupby(['region']).mean()['the_temp'].reset_index().sort_values('the_temp').tail(1)['region'].values[0]
        temperature = df.groupby(['region']).mean()['the_temp'].reset_index().sort_values('the_temp').tail(1)['the_temp'].values[0]           

        print(region)
        print(temperature)



        time_end  = datetime.now()

        return {'time': str(time_end-time_start),'region':region,'mean_temperature':temperature}




@api.route('/What_Are_The_Two_Drier_City_Region')
class What_Are_The_Two_Drier_City_Region(Resource):
    def get(self):

        time_start = datetime.now() 

        df = getDataFrame()

        df.humidity = df.humidity.astype(float)
        region_baja_humedad_1 = df.groupby(['region']).mean()['humidity'].reset_index().sort_values('humidity').head(2)['region'].values[0]
        humedad_1 = df.groupby(['region']).mean()['humidity'].reset_index().sort_values('humidity').head(2)['humidity'].values[0]  
        region_baja_humedad_2 = df.groupby(['region']).mean()['humidity'].reset_index().sort_values('humidity').head(2)['region'].values[1]
        humedad_2 = df.groupby(['region']).mean()['humidity'].reset_index().sort_values('humidity').head(2)['humidity'].values[1]            


        time_end  = datetime.now()

        return {'time': str(time_end-time_start),'primera_region':{region_baja_humedad_1:{'mean_humidity':humedad_1}},'segunda_region':{region_baja_humedad_2:{'mean_humidity':humedad_2}}}



@api.route('/What_Is_The_Hottes_Day_For_Each_Month_For_Each_City_Region')
class What_Is_The_Hottes_Day_For_Each_Month_For_Each_City_Region(Resource):
    def get(self):

        time_start = datetime.now() 

        df = getDataFrame()
        df.the_temp = df.the_temp.astype(float)
        df['mes'] = df.created.str.split(pat='-').apply(lambda x:x[1]).astype(int)
        df['dia']  = df.created.str.split(pat='-').apply(lambda x:x[2]).apply(lambda x: x[0:2]).astype(int)


        dia_region_1=[]
        dia_region_2=[]
        dia_region_3=[]
        for i in [1,2,3]:
            dia_region_1.append(df[(df.mes==i)&(df.region=='Buenos Aires')].groupby(['dia']).mean().reset_index().sort_values('the_temp').tail(1))
            dia_region_1[i-1]['region'] = 'Buenos Aires'
            dia_region_2.append(df[(df.mes==i)&(df.region=='Santiago')].groupby(['dia']).mean().reset_index().sort_values('the_temp').tail(1))
            dia_region_2[i-1]['region'] = 'Santiago'
            dia_region_3.append(df[(df.mes==i)&(df.region=='Brasilia')].groupby(['dia']).mean().reset_index().sort_values('the_temp').tail(1))
            dia_region_3[i-1]['region'] = 'Brasilia'          


        time_end  = datetime.now()

        return {    'time': str(time_end-time_start),


                    'primera_region':
                                        {
                                            'mes_1':{
                                                            'region':dia_region_1[0]['region'].iloc[0],
                                                            'dia':str(dia_region_1[0]['dia'].iloc[0]),
                                                            'temperature':str(dia_region_1[0]['the_temp'].iloc[0])
                                                    },
                                            'mes_2':{
                                                    'region':dia_region_1[1]['region'].iloc[0],
                                                    'dia':str(dia_region_1[1]['dia'].iloc[0]),
                                                    'temperature':str(dia_region_1[1]['the_temp'].iloc[0])
                                                    },
                                            'mes_3':{
                                                    'region':dia_region_1[2]['region'].iloc[0],
                                                    'dia':str(dia_region_1[2]['dia'].iloc[0]),
                                                    'temperature':str(dia_region_1[2]['the_temp'].iloc[0])
                                                    }


                                        },


                    'segunda_region':
                                        {
                                            'mes_1':{
                                                            'region':dia_region_2[0]['region'].iloc[0],
                                                            'dia':str(dia_region_2[0]['dia'].iloc[0]),
                                                            'temperature':str(dia_region_2[0]['the_temp'].iloc[0])
                                                    },
                                            'mes_2':{
                                                    'region':dia_region_2[1]['region'].iloc[0],
                                                    'dia':str(dia_region_2[1]['dia'].iloc[0]),
                                                    'temperature':str(dia_region_2[1]['the_temp'].iloc[0])
                                                    },
                                            'mes_3':{
                                                    'region':dia_region_2[2]['region'].iloc[0],
                                                    'dia':str(dia_region_2[2]['dia'].iloc[0]),
                                                    'temperature':str(dia_region_2[2]['the_temp'].iloc[0])
                                                    }


                                        },



                    'tercera_region':
                                        {
                                            'mes_1':{
                                                            'region':dia_region_3[0]['region'].iloc[0],
                                                            'dia':str(dia_region_3[0]['dia'].iloc[0]),
                                                            'temperature':str(dia_region_3[0]['the_temp'].iloc[0])
                                                    },
                                            'mes_2':{
                                                    'region':dia_region_3[1]['region'].iloc[0],
                                                    'dia':str(dia_region_3[1]['dia'].iloc[0]),
                                                    'temperature':str(dia_region_3[1]['the_temp'].iloc[0])
                                                    },
                                            'mes_3':{
                                                    'region':dia_region_3[2]['region'].iloc[0],
                                                    'dia':str(dia_region_3[2]['dia'].iloc[0]),
                                                    'temperature':str(dia_region_3[2]['the_temp'].iloc[0])
                                                    }


                                        },




                }



  

if __name__ == '__main__':
    app.wsgi_app = ProxyFix(app.wsgi_app)
    app.debug = True
    app.run(host='0.0.0.0', port=5000)



