# Imports
from mongo_handle import MongoHandle
import json

# Constants 
TARGET_COL = "cpdlCOL"
TARGET_PATH = f"E:\\Collection Data\\{TARGET_COL}.json"

# Get the instance of the MongoDB
db_handler = MongoHandle()
db = db_handler.get_client()

# Get instance of the specified collection
col = db["VIVY"][TARGET_COL]

# Load file
with open(TARGET_PATH) as file:
    file_data = json.load(file)

# Insert to collection
if isinstance(file_data, list):
    col.insert_many(file_data) 
else:
    col.insert_one(file_data)