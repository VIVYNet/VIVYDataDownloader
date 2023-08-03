"""
File Name:      data_handle.py

Authors:        Benjamin Herrera

Date Created:   11 FEB 2023

Date Modified:  11 FEB 2023

Description:    Script to download the data based on documents that have both
                links and text associated to themselves in the CPDL Collection
"""

# Imports
from mongo_handle import MongoHandle
from data_handle import DataHandle
from bs4 import BeautifulSoup
import concurrent.futures
import urllib.request

# Constants
TARGET_LOC = "Data/Raw"
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

    # Calculate the link to the page
    page = f"https://cpdl.org/wiki/index.php/{link}"

    # Get page's source information
    soup = BeautifulSoup(urllib.request.urlopen(page), "lxml")

    # Get page's poems
    poems = soup.find_all("div", class_="poem")

    # Return poems
    return [poem.text for poem in poems if len(poem.find_all("a")) == 0]


def process(document: dict) -> None:
    """Process Download Method

    Description:
        For the given dictionary instance, process the information for the
        downloading of content. This is utilized with multithreaded processes.

    Information:
        :param document: Dictionary/document to process for downloading
        :type document: dict
        :return: None
        :rtype: None
    """

    def insert_data():
        """Insert current song data to the database"""
        # Discard short texts. Some texts are just the name of a language.
        if len(text) < 20:
            return
        nonlocal count
        message = DATA_HANDLE.insert(
            method=1,
            title=title,
            composer=document["general_information"]["composer"][0],
            text=text,
            url=document["link"],
            links=document["download_links"][
                list(document["download_links"].keys())[0]
            ],
            custom_id=f"{document['_id']}_{count}",
        )
        count += 1
        print(message["Message"])

    # Print ID of the iterated document and get the correct text key
    print(f"--- {document['_id']} ---")
    key_text = "translations" if "translations" in document else "translation"

    # Get the title of document
    title = (
        document["general_information"]["title"][0]
        if "title" in document["general_information"]
        else document["title"].partition("(")[0]
    )
    count = 0
    # Iterate through the information listed in the translation
    for text_body in document[key_text]:
        # Attempt to process document
        try:
            # Iterate through the linktext's poems if the iterated information
            # is a linktext
            assert isinstance(document[key_text][text_body], list)
            texts = document[key_text][text_body]
            for text in texts:
                insert_data()

        # Catch and print errors
        except Exception:
            print("Error Occurred While Processing Document")


# Main run thread
if __name__ == "__main__":
    # Get a cursor with the documents that have links and text
    cursor = COL.find(
        {
            "$and": [
                {"translations": {"$gt": {}}},
                {"download_links": {"$gt": {}}},
            ]
        }
    )

    # MultiThreading process to quickly download content
    with concurrent.futures.ProcessPoolExecutor() as executor:
        _ = [executor.submit(process, i) for i in cursor]
    # for i in cursor: process(i)

    # Compile index.json file
    DATA_HANDLE.compile_index_and_errors()
