import os
import pymongo
from mongo_handle import MongoHandle
from data_handle import DataHandle
from bson.json_util import dumps

class CPDL_Parser():

    def __init__(self) -> None:

        #init mongocp
        mongo = MongoHandle()
        mongoClient = mongo.get_client()
        self.vivyDB = mongoClient.vivy
        self.CPDL = self.vivyDB.cpdl

    def CPDLquery(self):

        cursor = self.CPDL.find({ "$and": [ {"translations": {"$gt" : {} }}, { "download_links" : {"$gt": {}}}]}, {"translations": 3, "download_links": 3})
        templist = list(cursor)
        self.jsonData = dumps(templist, indent = 2)
        f = open('VIVY_RAW.json', 'w')
        f.write(self.jsonData)
        f.close()

    def cleanData(self):
        os.remove('VIVY_RAW.json')