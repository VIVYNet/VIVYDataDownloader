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

# Iterate through the cursor and insert data and information into the DB
# If the iterated text has only one item, the iterated instance is not placed into the 
for document in cursor:
    for text_body in document["translations"]:
        if len(document["translations"][text_body]) == 1:
            print(f"Can't Use as Text: {document['translation'][text_body][-1]}")
        else:
            DATA_HANDLE.insert(
                method=1, 
                text=document["translations"][text_body][-1],
                links=document["download_links"][list(document["download_links"].keys())[0]]
            )
            input()