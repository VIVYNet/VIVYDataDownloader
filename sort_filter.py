"""
File Name:      data_handle.py

Authors:        Benjamin Herrera

Date Created:   7 MAR 2023

Description:    Script to sort and filter out a downloaded dataset of digital
                representation
"""

# Imports
from data_handle import DataHandle
from tqdm import tqdm
import concurrent.futures
import json
import glob
import os

# Constants
SOURCE_LOC = "D:\\Projects\\VIVY\\Data\\Raw\\"
TARGET_LOC = "D:\\Projects\\VIVY\\Data\\Ready\\"
DATA_HANDLE = DataHandle(TARGET_LOC)
SOURCE_INDEX = json.load(open(f"{SOURCE_LOC}\\index.json"))
MUSESCORE = "D:\\Programs\\MuseScore\\bin\\MuseScore3.exe"


def process(item: dict) -> None:
    """Process Method

    Description:
        For the given dictionary instance, copy or compile MIDI files to
        transfer over to a DB that handles filtered and sorted MIDI files

    Information:
        :param item: Dictionary/document to process for filtering/sorting
        :type item: dict
        :return: None
        :rtype: None
    """

    # Get file paths that have the ".mid" file type
    mid_files = glob.glob(
        f"{SOURCE_LOC}\\data\\{item['_id']}\\*.mid"
    ) + glob.glob(f"{SOURCE_LOC}\\data\\{item['_id']}\\*.midi")
    mxl_files = glob.glob(f"{SOURCE_LOC}\\data\\{item['_id']}\\*.mxl")

    # Copy file to the target DB if the mid_files list is not empty
    if mid_files != []:
        DATA_HANDLE.copy(from_path=mid_files[0], index_doc=item)

    # If no MIDI files are present, compile MIDI file from an existing MXL file
    elif mxl_files != []:
        filename = (
            mxl_files[-1].split("\\")[-1].split(".")[0]
        )  # Get name of the MXL file

        filepath = "\\".join(
            mxl_files[-1].split("\\")[:-1]
        )  # Get file path to the MXL file

        # Try to convert and copy data
        try:
            os.system(
                f"{MUSESCORE} {mxl_files[0]} -o {filepath}\\{filename}.mid"
            )  # Compile the MXL file

            # Copy data
            DATA_HANDLE.copy(
                from_path=f"{filepath}\\{filename}.mid", index_doc=item
            )

            os.remove(f"{filepath}\\{filename}.mid")  # Delete compiled file

        # Catch error and handle
        except Exception as e:
            # Call error handle method
            DATA_HANDLE.error_handle(
                data=item, error=str(e), link=mxl_files[0]
            )

            return  # return

    # Report error if no file was file
    else:
        DATA_HANDLE.error_handle(
            data=item,
            error="No .MID, .MIDI, or .MXL file found",
            link=item["_id"],
        )

    return  # return


# Main run thread
if __name__ == "__main__":
    # MultiThreading process to quickly download content
    with concurrent.futures.ProcessPoolExecutor(max_workers=8) as executor:
        _ = list(
            tqdm(executor.map(process, SOURCE_INDEX), total=len(SOURCE_INDEX))
        )

    DATA_HANDLE.compile_index_and_errors()  # Compile index into a JSON file

    # Print message that sorting was complete
    print("Sorting and Filtering Completed")
