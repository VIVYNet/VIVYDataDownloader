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
import urllib

class DataHandle():
    """Data Handling Class
    
    Description:
        This class is responsible for the downloading, indexing, and storing
        of any data.
    
    Methods:
        
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
        
        Information:
            :param path: Path to the folder
            :type path: str
            :return: None
            :rtype: None
        """
        
        self.PATH = os.path.abspath(path)   # Store the path as an absolute path
        
        
    
