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
from bs4 import BeautifulSoup
import urllib.request
import json

# Constants
TARGET_LOC = "E:\\Data\\"
DATA_HANDLE = DataHandle(TARGET_LOC)
MONGO_DB = MongoHandle()
COL = MONGO_DB.get_client()["VIVY"]["cpdlCOL"]

def link_parser(link: str) -> list:
    """Link Parser Method
    
    Description:
        Get text bodies from a link text. Parses the information using BS4.
        
    Information:
        :param link: String of the link to the linktext
        :type link: str
        :return: list of translated text
        :rtype: list[str]
    """
    
    page = f"https://cpdl.org/wiki/index.php/{link}"    # Calculate the link to the page
    
    soup = BeautifulSoup(urllib.request.urlopen(page), "lxml")  # Get page's source information
    
    poems = soup.find_all("div", class_="poem")     # Get page's poems
    
    return [poem.text for poem in poems if len(poem.find_all("a")) == 0]    # Return poems


# Main run thread
if __name__ == "__main__":
    
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
    for document in cursor:
        
        # Print ID of the iterated document and get the correct text key
        print(f"--- {document['_id']} ---")
        key_text = "translations" if "translations" in document else "translation"
        
        # Iterate through the information listed in the translation
        for text_body in document[key_text]:
            
            # Iterate through the linktext's poems if the iterated information is a linktext
            if document[key_text][text_body][0] == document["general_information"]["title"][0]:
                texts = link_parser(document[key_text][text_body][0].replace(" ", "_"))
                for text in texts:
                    try: 
                        message = DATA_HANDLE.insert(
                            method=1, 
                            title=document["general_information"]["title"][0],
                            composer=document["general_information"]["composer"][0],
                            text=text,
                            links=document["download_links"][list(document["download_links"].keys())[0]]
                        )
                        print(message["Message"])
                    except Exception as e:
                        print(f"Error Occurred While Processing Document")
                
            # Casually download the data and print an error message if something goes right
            else:
                try: 
                    message = DATA_HANDLE.insert(
                        method=1, 
                        title=document["general_information"]["title"][0],
                        composer=document["general_information"]["composer"][0],
                        text=document[key_text][text_body][-1],
                        links=document["download_links"][list(document["download_links"].keys())[0]]
                    )
                    print(message["Message"])
                except Exception as e:
                    print(f"Error Occurred While Processing Document")