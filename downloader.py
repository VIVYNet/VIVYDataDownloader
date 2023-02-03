import os
import json
import urllib
import requests
from bson.json_util import dumps
import os
import pymongo
from mongo_handle import MongoHandle
from data_handle import DataHandle
from cpdl_parser import CPDL_Parser

path = input("Enter path for data folder:\n")

cpdlParse = CPDL_Parser()
cpdlParse.CPDLquery()
cpdlPare.cleanData()

handler = DataHandle(path)
handler.handle(1)