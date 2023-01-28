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
import uuid
import urllib

VERSION = "v1.0.0"  # Versioning for the documents

class DataHandle():
    """Data Handling Class
    
    Description:
        This class is responsible for the downloading, indexing, and storing
        of any data.
    
    Methods:
        DataHandle(str) -> None
        insert(int, str, str) -> dict
    """
    
    def __init__(self, path: str) -> None:
        """Constructor for Data Handling Class
        
        Description:
            Creates an instance to manipulate information and data of a folder that 
            contains the data that can and is meant to be manipulable with this class. 
            
            Structure of the folder should look like this:
            
            <path_to_folder>
            ├───index.json
            └───data
                ├───<document_folder_1>
                ├───<document_folder_2>
                └───<...>

            The path parameter must have the "/" character to standardize discrepancies
            in path naming.
        
        Information:
            :param path: Path to the folder
            :type path: str
            :return: None
            :rtype: None
        """
        
        # Create the data directory, index.json file, and data subdirectories
        # if it doesn't exist
        if not os.path.exists(path):

            # Make the DB path
            os.makedirs(path)
            os.makedirs(path + "data/")

            # Creation of index.json
            self.index_file = open(path + "index.json", "a+")
            self.index_file.close()


        # Read the file and open it in "r+" mode and get its JSON/dict representation
        self.index_file = open(path + "index.json", "r+")
        self.index = json.load(self.index_file)
        
        # Else, read the file
        self.PATH = os.path.abspath(path)   # Store the path as an absolute path
        
        
    def insert(self, method: int, text: str, links: list[str]) -> dict:
        """Document Insertion Method Using Download Link
        
        Inserts a datapoint or document into the DB. This method also updates
        the index.json to keep track of changes. Items inside index.json will
        contain:

        1. An unique ID for the document
        2. The methodology used to insert the document
        3. The associating text to the datapoint
        4. A filepath pointer to the data in the DB
        5. A version to track what structure is the document in the index.json

        An example of the structure for an item in the index.json is provided below
        
        "c5741947f6ab466ca59fdd5853c8d779": {
            "id": "c5741947f6ab466ca59fdd5853c8d779"
            "method": 1,
            "text": "Lorem Ipsum...",
            "directory": "./data/c5741947f6ab466ca59fdd5853c8d779",
            "version": "v1.0.0"
        }

        The method will return a dict with True if more than zero files were downloaded. 
        False if otherwise. The structure of the dict is found below:

        {
            "status": True,
            "Message": "Completed document insertion.\n1/2 documents downloaded."
        }

        Information:
            :param method: The methodology used to gather the information of the datapoint
            :type method: int
            :param text: The text associated to the datapoint
            :type text: str
            :param links: The links to download the digital content for the datapoint
            :type links: list[str]
            :return: Returns the status and a message of the insertion process
            :rtype: dict
        """

        # Variable declaration and initialization
        num_links = len(links)
        num_downloads = 0

        id = str(uuid.uuid4()).replace("-", "")     # Create an unique ID 

        os.makedirs(f"{self.PATH}/data/{id}/")      # Create the datapoint's subdirectory into the DB

        # Update information in the index
        self.index[id] = {
            "id": id,
            "method": method,
            "text": text,
            "directory": f"./data/{id}/",
            "version": VERSION
        }

        # Iterate through the links and try to download them
        for i in links:

            # Try to download the iterated link
            try:
                file_name = i.split("/")[-1]
                urllib.urlretrieve(i, f"{self.PATH}/data/{id}/{file_name}")

            # Catch the exception and increment the download count
            except:
                num_downloads += 1
        
        # Return a success message
        return {
            "Status": True if num_downloads else False,
            "Message": f"Completed document insertion.\n{num_downloads}/{num_links} downloaded."
        }