from pymongo import MongoClient
import json

myclient = MongoClient('localhost', 27017)
myData = myclient['assignment2']

f1 =  open('table1.json', encoding='utf-8')
f2 =  open('table2.json', encoding='utf-8')
Table1 = json.load(f1)
Table2 = json.load(f2)
print("data loaded!")

# myData['assignment2']['table1'].drop()
# myData['assignment2']['table2'].drop()

myData['table1'].insert_many(Table1)
myData['table2'].insert_many(Table2)
print('done')