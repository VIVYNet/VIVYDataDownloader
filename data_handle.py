"""
File Name:      data_handle.py

Authors:        Benjamin Herrera

Date Created:   21 JAN 2023

Date Modified:  21 JAN 2023

Description:    Class to handle the downloading and storing of large files, while
                also indexing miscellaneous information.
"""

# Imports
import os
import json
import urllib
import requests
from bson.json_util import dumps
from time import sleep

class DataHandle():
    """Data Handling Class
    
    Description:
        This class is responsible for the downloading, indexing, and storing
        of any data.
    
    Methods:
        
    """
    
    def __init__(self, path: str) -> None:
        # """Constructor for Data Handling Class
        
        # Description:
        #     Creates an instance to manipulate information and data of a folder that 
        #     contains the data that can and is meant to be manipulable with this class. 
            
        #     Structure of the folder should look like this:
            
        #     <path_to_folder>
        #     ├───index.json
        #     └───data
        #         ├───<document_folder_1>
        #         ├───<document_folder_2>
        #         └───<...>
        
        # Information:
        #     :param path: Path to the folder
        #     :type path: str
        #     :return: None
        #     :rtype: None
        # """
        path = os.path.join(path, "VIVYDATA")
        self.PATH = os.path.abspath(path)   # Store the path as an absolute path
        os.mkdir(self.PATH) # makes the directory for the data

        self.indexPath = os.path.join(self.PATH, 'index.json')
        f = open(self.indexPath, "w")
        f.close()

        self.dataPath = os.path.join(self.PATH, "data")
        os.mkdir(self.dataPath)

        
        
    def createIndex(self, _id, text, id_path, method):
        #creates and formats index.json file after data is handled
        # """
        # "id": {
        #     "id": <Unique ID>
        #     "method": <1 for using the CPDL method and 2 for using the workable method>
        #     "text": <Text of the datapoint>
        #     "directory": <file path pointer to the directory that contains the data>
        #     "version": <Version of the document structure inside index.json>
        #     }
        # """
        jsonDict = {
            "id": {
                "id": _id,
                "method": method,
                "text": text,
                "directory": id_path,
                "version": 0.1
                }
        }
        f = open(self.indexPath, 'a')
        jString = json.dumps(jsonDict, indent = 2)
        f.write(jString)
        f.close()



    def download_data(self, idPath, dloadLinks, _id):

        for link in dloadLinks:

            tokenBeg = link.rfind('.')
            tokenEnd = len(link)
            token = _id + link[tokenBeg:tokenEnd]

            tokenPath = os.path.join(idPath, token)
            #sleep(0.05)
            try:
                response = requests.get(link, timeout=5)
                temp = response
                response.close()
                with open(tokenPath, 'wb') as f:
                    f.write(temp.content)
            except:
                print("Error with: " + link + "\nIn " + _id)
                continue
            


    def handle(self, method):

        with open('VIVY_RAW.json', 'r') as f:
            vivy_dictionary = json.loads(f.read())
        for data_obj in vivy_dictionary:

            _id = data_obj["_id"]
            _translations = data_obj["translations"]
            temp = data_obj["download_links"]
            keyList = list(temp.keys())
            _dlLinks = temp[keyList[0]]

            id_path = os.path.join(self.dataPath, _id)
            os.mkdir(id_path)

            self.createIndex(_id, _translations, id_path, method)
            self.download_data(id_path, _dlLinks, _id)
