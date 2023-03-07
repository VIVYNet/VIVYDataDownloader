"""
File Name:      data_handle.py

Authors:        Benjamin Herrera

Date Created:   21 JAN 2023

Date Modified:  28 JAN 2023

Description:    Class to handle the downloading and storing of large files, while
                also indexing miscellaneous information.
"""

# Imports
import os
import re
import json
import uuid
import shutil
import urllib.request
from bson.json_util import dumps
from mongo_handle import MongoHandle

VERSION = "v1.1.0"  # Versioning for the documents

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
            self.index_file = open(path + "index.json", "a+", encoding="utf-8")
            self.index_file.write("[{\"_id\": -1, \"track\": 1}]")
            self.index_file.close()

        self.PATH = os.path.abspath(path)   # Store the path as an absolute path

        self.index = json.load(open(path + "index.json", "r+"))     # Get the contents of index.json
        
        # Establish MongoDB DB connection for temp holding
        self.MONGO_DB = MongoHandle()
        self.COL = self.MONGO_DB.get_client()["VIVYDownload"]["temp"]
        
        self.COL.drop()     # Empty collection
        
        # Recreate collection and seed it with index.json information
        self.COL = self.MONGO_DB.get_client()["VIVYDownload"]["temp"]
        self.COL.insert_many(self.index)
    
    def compile_index(self) -> None:
        """Index Compile Method
        
        Description:
            Compile temp data from MongoDB temp collection to index.json file
        
        Information:
            :return: None
            :rtype: None           
        """
        
        cursor = self.COL.find({})  # Generate cursor
        
        # Overwrite index.json file for writing
        with open(f"{self.PATH}\\index.json", 'w', encoding="utf-8") as file:
            json.dump(json.loads(dumps(cursor)), file)
    
    def insert(self, method: int, title: str, composer: str, text: str, links: list, custom_id=None) -> dict:
        """Document Insertion Method Using Download Link
        
        Description:
            Inserts a datapoint or document into the DB. This method also updates
            the index.json to keep track of changes. Items inside index.json will
            contain:

                1. An unique ID for the document
                2. The name of the song
                3. The composer of the song
                4. The methodology used to insert the document
                5. The associating text to the datapoint
                6. A filepath pointer to the data in the DB
                7. A version to track what structure is the document in the index.json

            An example of the structure for an item in the index.json is provided below
            
                "c5741947f6ab466ca59fdd5853c8d779": {
                    "id": "c5741947f6ab466ca59fdd5853c8d779",
                    "title": "Free Bird 2",
                    "composer": "Your Mother",
                    "method": 1,
                    "text": "Lorem Ipsum...",
                    "directory": "./data/c5741947f6ab466ca59fdd5853c8d779",
                    "version": "v1.0.0"
                }

            The method will return a dict with True if more than zero files were downloaded. 
            False if otherwise. The structure of the dict is found below:

                {
                    "Status": True,
                    "ID": "c5741947f6ab466ca59fdd5853c8d779",
                    "Message": "Completed document insertion.\n1/2 documents downloaded."
                }

        Information:
            :param method: The methodology used to gather the information of the datapoint
            :type method: int
            :param title: The name of the song to insert into the database
            :type title: str
            :param composer: The composer of the song
            :type composer: str
            :param text: The text associated to the datapoint
            :type text: str
            :param links: The links to download the digital content for the datapoint
            :type links: list[str]
            :param custom_id: The ID the callee would like to use instead of a random one
            :type custom_id: str
            :return: Returns the status and a message of the insertion process
            :rtype: dict
        """

        num_downloads = 0   # Variable declaration and initialization

        id = str(uuid.uuid4()).replace("-", "") if custom_id == None else custom_id    # Create an unique ID 

        os.makedirs(f"{self.PATH}/data/{id}/")      # Create the datapoint's subdirectory into the DB

        # Iterate through the links and try to download them
        for i in links:

            # Try to download the iterated link and increment download count
            try:
                file_name = i.split("/")[-1]
                urllib.request.urlretrieve(i, f"{self.PATH}/data/{id}/{file_name}")
                num_downloads += 1

            # Catch the exception and print the error, delete folder, and return
            except Exception as e:
                print(f"Can't Download: {i}")               # Print error message
                shutil.rmtree(f"{self.PATH}/data/{id}/")    # Remove folder
                return                                      # Return
        
        # Insert new information to the temp MongoDB collection
        self.COL.insert_one({
            "_id": id,
            "title": re.sub('[^A-Za-z0-9 ]+', '', title).lower(),
            "composer": composer.lower(),
            "method": method,
            "text": text,
            "directory": f"./data/{id}/",
            "version": VERSION
        })
        
        # Return a success message
        return {
            "Status": True if num_downloads else False,
            "ID": id,
            "Message": f"Completed document insertion. {num_downloads}/{len(links)} files downloaded."
        }

    def update(self, id: str, **kwargs: object) -> dict:
        """Document Update Method

        Description:        
            Updates a document's information in the index.json and other specified options.
            These options includes:

                1. Changing the text of the data
                2. Change title of song
                3. Change composer of song
                4. Adding downloaded content for a document
                5. Replacing downloaded content for a document (clear and replace)
                6. Update the methodology (pertains to the method field)
                7. Provide additional information
                8. Update the version of the information for a document in index.json

            The following are the parameters to declare to achieve the above options in order:

                1. text: str
                2. title: str
                3. composer: str
                4. links: list[str], add: bool = True
                5. links: list[str], add: bool = False
                6. method: int
                7. additional: dict
                8. version: str

            Note, options 2 and 3 cannot be acted as they are completely different from one another.

            Here is an example of calling this method to (1) replace a document's downloaded 
            content, (2) add new index information to the document, and (3) change the document's
            index information version:

                handle = MongoHandle("./")
                doc = handle.insert(
                    method = 1, 
                    text="Lorem Ipsum Dolor Sit", 
                    links=[
                        "www.site.com/link.mid",
                        "www.site.com/link.mxl"
                    ]
                )
                handle.update(  
                    id=doc["ID"],
                    links=[
                        "www.website.com/song.mid",
                        "www.website.com/song.mxl",
                        "www.website.com/song.pdf"
                    ],
                    add=False,
                    additional={
                        "replaced": True,
                        "author": "LT COL D. Shane Richardson"
                    },
                    version="1.25.0"
                )

            For other specifications, refer to the information section of this docstring.

        Information:
            :param id: The ID of the document the call wants to change
            :type id: str
            :param **kwargs: The different options that the call would like to specify
            :type **kwargs: object
                ├───:param text: Text to replace in the document
                ├───:type text: str
                ├───:param title: Title of the song to change
                ├───:type title: str
                ├───:param composer: Composer of the song to change
                ├───:type composer: str
                ├───:param links: List of links to download
                ├───:type links: list[str]
                ├───:param add: Specification to whether add or replace files in the DB
                ├───:type add: bool
                ├───:param method: Method to change
                ├───:type method: int
                ├───:param additional: Dictionary containing additional information to add
                ├───:type additional: dict
                ├───:param version: Version to update document on
                ├───:type version: str
            :return: Returns the status and a message of the update process
            :rtype: dict
        """

        # Variable declaration
        num_downloads = 0
        success_message = "Update success."

        # If the ID is not in index.json, return a false message
        if id not in self.index:
            return {
                "Status": False,
                "Message": "ID not found in DB"
            }
        
        # If links was provided, but not add, return a false message
        if "links" not in kwargs and "add" not in kwargs:
            return {
                "Status": False,
                "Message": "\"add\" parameter not specified"
            }

        # Iterate through the other parameters if specified
        for item in kwargs:

            # Add additional information if iterated item is "additional" and update success message
            if item == "additional":
                for field in kwargs["additional"]:
                    self.index[id][field] = kwargs["additional"][field]
                    success_message += f"\nAdded \"{field}\" information"
            
            # Add or Replace the downloaded content if the iterated item is "links"
            if item == "links":
                
                # If add is false, delete the folder and recreate it
                if not kwargs["add"]:
                    shutil.rmtree(f"{self.PATH}/data/{id}/")
                    os.makedirs(f"{self.PATH}/data/{id}/") 

                # Iterate through the links and try to download them
                for i in kwargs["links"]:

                    # Try to download the iterated link and increment download count
                    try:
                        file_name = i.split("/")[-1]
                        urllib.request.urlretrieve(i, f"{self.PATH}/data/{id}/{file_name}")
                        num_downloads += 1

                    # Catch the exception and print the error
                    except Exception as e:
                        print(f"{id} - Can't Download: {i}")
                
                # Update the success message
                num_links = len(kwargs["links"])
                success_message += f"\n{num_downloads}/{num_links} files downloaded."
            
            # Directly change the document's field if otherwise and update success message
            else:
                self.index[id][item] = kwargs[item]
                success_message += f"\nUpdated \"{field}\" information"

        # Write changes
        self.__write_index()

        # Return a message
        return {
            "Status": True,
            "ID": id,
            "Message": success_message
        } 