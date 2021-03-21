import requests
import pandas as pd
from pymongo import MongoClient
import json

df = pd.read_csv('st_regiones.csv',sep='|').iloc[:,1:]

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
client = MongoClient("mongodb+srv://mongo:mongo@cluster0.yjbeg.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
db = client.get_database('test')
data = db.data_st_regiones 
data.drop()


##SAVE ON MONGO
df = df.loc[:,:]
records = json.loads(df.to_json()).values()
data.insert(records)
