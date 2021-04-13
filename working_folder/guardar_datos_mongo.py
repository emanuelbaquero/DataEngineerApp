import requests
import pandas as pd
from pymongo import MongoClient
import json

df = pd.read_csv('st_regiones.csv',sep='|').iloc[:,1:]


client = MongoClient("mongodb+srv://mongo:mongo@cluster0.yjbeg.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
db = client.get_database('test')
data = db.data_st_regiones 
data.drop()


##SAVE ON MONGO
df = df.loc[:,:]
records = json.loads(df.to_json()).values()
data.insert(records)
