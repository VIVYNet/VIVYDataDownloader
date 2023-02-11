import os
import pymongo
import json
from mongo_handle import MongoHandle
from bson.json_util import dumps


mongo = MongoHandle()
mongoClient = mongo.get_client()
vivyDB = mongoClient.vivy
imslp = vivyDB.imslp
workable = vivyDB.workable

pipeline = [
    {
        "$lookup":
            {
            "from": "imslp",
            "localField": "songs.0.0",
            "foreignField": "information.Work Title",
            "as": "res"
            }
    },
    {
        "$match":
            {
            "res": {"$gt" : {} }
            }
    }
]

results = workable.aggregate(pipeline)
templist = list(results)
jsonData = dumps(templist, indent = 2)

f = open('ag.json', 'w')
f.write(jsonData)
f.close()