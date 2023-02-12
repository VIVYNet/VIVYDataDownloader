"""
File Name:      data_handle.py

Authors:        Benjamin Herrera

Date Created:   11 FEB 2023

Date Modified:  11 FEB 2023

Description:    Script to download the data based on documents that have both links and text associated
                to themselves in the CPDL Collection
"""

# Imports
from mongo_handle import MongoHandle
from data_handle import DataHandle
import json

# Constants
TARGET_LOC = "E:\\Data\\"
DATA_HANDLE = DataHandle(TARGET_LOC)
MONGO_DB = MongoHandle()
COL = MONGO_DB.get_client()["VIVY"]["cpdlCOL"]

# Get a cursor with the documents that have links and text
cursor = COL.find(
    { "$and": 
        [ 
            {"translations": {"$gt" : {} }}, 
            {"download_links": {"$gt": {}}}
        ]
    }
)

# Iterate through the cursor and insert into the DB
for document in cursor:
    for text_body in document["translations"]:
        print(json.dumps(document["general_information"]["title"], indent=4))
        print(json.dumps(document["translations"][text_body], indent=4))
        input("\n")