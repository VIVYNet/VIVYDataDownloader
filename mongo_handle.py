"""
File Name:      mongo.py

Authors:        Benjamin Herrera

Date Created:   19 JAN 2023

Date Modified:  19 JAN 2023

Description:    Class to handle MongoDB access and manipulation of collection
"""

# Imports
import json
import pymongo


MONGO_BASE_URI = "mongodb://"       # Constant variable for Mongo URI 

class MongoHandle():
    """Mongo Handling Class
    
    Description:
        Handles the reading, parsing, and manipulation of a mongoDD.
        Utilizes a file pointer to a JSON file to parse login information.
    
    Methods:
        MongoHandle(str) -> None
        get_client() -> pymongo.MongoClient
    """
    
    def __init__(self, file: str="login.json") -> None:
        """Constructor for Mongo Handling Class
        
        Description:
            Parses the given json file and assign values to the following
            instance variables: (1) Address, (2) Port, (3) Username, (4) Password.
        
        Information:
            :param file: File to parse for MongoDB connection
            :type file: str
            :return: None
            :rtype: None     
        """
        
        login_info = json.load(open(file))  # Open the JSON file
        
        # Read file and assign values to the following instance variables
        self.ADDRESS = login_info["address"]
        self.PORT = login_info["port"]
        self.USERNAME = login_info["username"]
        self.PASSWORD = login_info["password"]
        
        # Connect to the MongoDB instance based on the given information.
        # If a localhost connection is specified, then the `client` instance variable
        # will connect directly to localhost's MongoDB instance. Else if otherwise.
        if self.ADDRESS.lower() == "localhost":
            self.client = pymongo.MongoClient(f"{MONGO_BASE_URI}localhost/")
        else:
            self.client = pymongo.MongoClient(f"{MONGO_BASE_URI}{self.USERNAME}:{self.PASSWORD}@{self.ADDRESS}:{self.PORT}/")
    
    def get_client(self) -> pymongo.MongoClient:
        """Getter Method for the Connected Client
        
        Description:
            Returns the Pymongo MongoClient instance of the connected MongoDB
            instance. Parsable for a multitude of things.
            
        Information:
            :return: Pymongo MongoClient instance of the connected MongoDB
            :rtype: pymongo.MongoClient   
        """
        
        return self.client  # Return the mongo client instance
    
    

        
        
    

