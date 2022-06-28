# -*- coding: utf-8 -*-
"""
Created on Mon Jun 27 22:52:16 2022

@author: TEVIN
"""

import redis
import pandas as pd
import pymongo
from pymongo import MongoClient
import time
#from pymongo_test_insert import get_database
################

CONNECTION_STRING = "mongodb+srv://toloo:Godocholoo@cluster0.idj1r.mongodb.net/test"
#"mongodb+srv://<username>:<password>@<cluster-name>.mongodb.net/myFirstDatabase"

rc = redis.Redis(
    host="redis-15365.c277.us-east-1-3.ec2.cloud.redislabs.com",
    charset="utf-8",
    decode_responses=True,
    port=15365,
    password="ruqK6OVaQpYajpp1glVruZYZdTHQfMlq")


client = MongoClient(CONNECTION_STRING)
dbname = client['iotdatadb']
collection_name = dbname["iotdatacoll"]

def insertData():
    #Fetching from db
    df = pd.DataFrame()
    p = rc.pipeline()
    for key in rc.keys():
        p.hgetall(key)
    #df = df.append
    df = df.append(p.execute())
    #iotdata = p.execute()   
    
    #collection_name.insert_many(iotdata)
   
    iotdata = df.to_dict('records')
    #print(iotdata)
    for iotdata in iotdata:
             
        try:
            collection_name.insert_one(iotdata)
            #mycol_prod_hist.insert_one(x)
            #print (x)
        except pymongo.errors.DuplicateKeyError:
            # skip document because it already exists in new collection
            continue                

   
if __name__ == '__main__':
    insertData()
    time.sleep(90000)
