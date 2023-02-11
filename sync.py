# Imports
from mongo_handle import MongoHandle
from bson.json_util import dumps
import json

# Constants 
TARGET_COL = "workableCOL"
TARGET_PATH = f"E:\\Collection Data\\{TARGET_COL}.json"

# Get the instance of the MongoDB
db_handler = MongoHandle()
db = db_handler.get_client()

# Get instance of the specified collection
col = db["VIVY"][TARGET_COL]

# Get cursor
cursor = col.find({})
with open(TARGET_PATH, 'w') as file:
    json.dump(json.loads(dumps(cursor)), file)
