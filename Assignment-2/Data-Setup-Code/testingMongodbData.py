from pymongo import MongoClient
import json
from pprint import pprint

myclient = MongoClient('localhost', 27017)
myData = myclient['assignment2']

Table1 = myData['table1']
Table2 = myData['table2']

result = Table1.find_one()
pprint(result)

result = Table2.find_one()
pprint(result)