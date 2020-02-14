import csv, json
from tqdm import tqdm

# --------------------- Data 1 ---------------
ColumnNames = ['logging_level', 'timestamp', 'downloader_id', 'retrieval_stage', 'request_status', 'url', 'access_key']
Table1 = []
f = open('NewData.csv', encoding='utf-8')
data = csv.reader(f, delimiter=',')
for line in tqdm(data):
    doc = {}
    for i in range(len(ColumnNames)):
        doc[ColumnNames[i]] = line[i]
    Table1.append(doc)
f.close()

# --------------------- Data 2 --------------
ColumnNames = ['id', 'url', 'owner_id', 'name', 'language', 'created_at', 'forked_from', 'deleted', 'updated_at']
Table2 = []
f = open('important-repos.csv', encoding='utf-8')
data = csv.reader(f, delimiter=',')
for line in tqdm(data):
    doc = {}
    for i in range(len(ColumnNames)):
        doc[ColumnNames[i]] = line[i]
    Table2.append(doc)
f.close()

# -------------------- Dump Data in json --------
f = open('table1.json', 'w')
json.dump(Table1, f, sort_keys=True, indent=4)
f.close()

f = open('table2.json', 'w')
json.dump(Table2, f, sort_keys=True, indent=4)
f.close()

pyspark --conf "spark.mongodb.input.uri=mongodb://127.0.0.1:27017/assignment2.table2?readPreference=primaryPreferred" --conf "spark.mongodb.output.uri=mongodb://127.0.0.1:27017/assignment2.table2" --packages org.mongodb.spark:mongo-spark-connector_2.11:2.4.1