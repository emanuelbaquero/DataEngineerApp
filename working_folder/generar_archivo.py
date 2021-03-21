#!/bin/python3

import requests
import pandas as pd
from pymongo import MongoClient
import json


def get_dataframe_api(mes, dias):
    response_bs_as = requests.get("https://www.metaweather.com/api/location/search/?query=Buenos Aires")
    response_brasilia = requests.get("https://www.metaweather.com/api/location/search/?query=Brasília")
    response_santiago = requests.get("https://www.metaweather.com/api/location/search/?query=Santiago")
    
    id_BuenosAires = int(pd.DataFrame(response_bs_as.json()).woeid)
    id_Brasilia = int(pd.DataFrame(response_brasilia.json()).woeid)
    id_Santiago = int(pd.DataFrame(response_santiago.json()).woeid)
    
    for i in range(1,dias):
        STR_GET = "https://www.metaweather.com/api/location/{}/2021/{}/{}/".format(id_BuenosAires,mes,i)
        response = requests.get(STR_GET)
        if i==1:
            df_bsas = pd.DataFrame(response.json())
        else:
            df_bsas_aux = pd.DataFrame(response.json())
            df_bsas = pd.concat([df_bsas,df_bsas_aux])
    df_bsas['region'] = 'Buenos Aires'
            
    for i in range(1,dias):
        STR_GET = "https://www.metaweather.com/api/location/{}/2021/{}/{}/".format(id_Brasilia,mes,i)
        response = requests.get(STR_GET)
        if i==1:
            df_brasilia = pd.DataFrame(response.json())
        else:
            df_brasilia_aux = pd.DataFrame(response.json())
            df_brasilia = pd.concat([df_brasilia, df_brasilia_aux])
    df_brasilia['region'] = 'Brasilia'
            
    for i in range(1,dias):
        STR_GET = "https://www.metaweather.com/api/location/{}/2021/{}/{}/".format(id_Santiago,mes,i)
        response = requests.get(STR_GET)
        if i==1:
            df_santiago = pd.DataFrame(response.json())
        else:
            df_santiago_aux = pd.DataFrame(response.json())
            df_santiago = pd.concat([df_santiago,df_santiago_aux])
            
    df_santiago['region'] = 'Santiago'
        
    df = pd.concat([df_bsas,df_brasilia,df_santiago])
            
    return df



df_enero = get_dataframe_api(1,27)
df_febrero = get_dataframe_api(2,27)
df_marzo = get_dataframe_api(3,27)

df = pd.concat([df_enero,df_febrero,df_marzo])
df.to_csv('st_regiones.csv',sep='|')
