from data_handle import DataHandle
from cpdl_parser import CPDL_Parser

path = input("Enter path for data folder:\n")

cpdlParse = CPDL_Parser()
cpdlParse.handle(path)

#workableParse = Workable_Parser()
#workableParse.handle(path)