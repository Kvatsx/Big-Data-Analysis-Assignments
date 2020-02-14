from pymongo import MongoClient
import json
from pprint import pprint

myclient = MongoClient('localhost', 27017)
myData = myclient['assignment2']


Table1 = myData['table1']
# Table1.create_index('DOWNLOADER_ID')
Table1.drop_index('DOWNLOADER_ID_1')