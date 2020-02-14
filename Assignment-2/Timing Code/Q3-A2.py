#!/usr/bin/env python
# coding: utf-8

# In[29]:


import os
import time

import findspark
findspark.init()
findspark.find()

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext, Row
import numpy as np

num_executors = 2
memory = "100g"
pyspark_submit_args = '--driver-memory ' + memory + ' pyspark-shell ' + '--num-executors ' + str(num_executors)
os.environ["PYSPARK_SUBMIT_ARGS"] = pyspark_submit_args

# In[2]:


conf = SparkConf().setAppName("myApp").setMaster("local[*]")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)


# In[3]:


df_table1 = sc.textFile("hdfs://localhost:9000/mydata/Table1.csv")
df_table2 = sc.textFile("hdfs://localhost:9000/mydata/Table2.csv")


# In[4]:


df_table1.first()


# In[5]:


df_table2.first()


# In[6]:


parts1 = df_table1.map(lambda l: l.split(","))
parts2 = df_table2.map(lambda l: l.split(","))

table1 = parts1.map(lambda p: Row(     logging_level=str(p[0]),     timestamp=str(p[1]),     downloader_id=str(p[2]),     retrieval_stage=str(p[3]),     request_status=str(p[4]),     url=str(p[5]),     access_key=str(p[6])))

# id,url,owner_id,name,language,created_at,forked_from,deleted,updated_at
table2 = parts2.map(lambda p: Row(     id=str(p[0]),     url=str(p[1]),     owner_id=str(p[2]),     name=str(p[3]),     language=str(p[4]),     created_at=str(p[5]),     forked_from=str(p[6]),     deleted=str(p[7]),     updated_at=str(p[8])))


# In[7]:


print("Table1")
schemaTable1 = sqlContext.createDataFrame(table1)
schemaTable1.printSchema()
schemaTable1.createOrReplaceTempView("GHTORRENT_RECORDS")

print("Table2")
schemaTable2 = sqlContext.createDataFrame(table2)
schemaTable2.printSchema()
schemaTable2.createOrReplaceTempView("IMPORTANT_REPOS")


# In[8]:


# result = sqlContext.sql("Select * from GHTORRENT_RECORDS limit 10")


# In[9]:


# result.show()


# ## Queries

# In[38]:


query2 = "SELECT COUNT(*) FROM GHTORRENT_RECORDS"
start_time = time.time()
result2 = sqlContext.sql(query2)
# print("How many records does the table contain?") 
# result2.show()


# In[39]:


query3 = "SELECT COUNT(*) FROM GHTORRENT_RECORDS WHERE LOGGING_LEVEL='WARN'"
result3 = sqlContext.sql(query3)
# print("Count the number of WARNing messages.")
# result3.show()


# In[41]:


# 78588
query4 = '''SELECT COUNT(DISTINCT URL) 
               FROM GHTORRENT_RECORDS 
               WHERE RETRIEVAL_STAGE = 'api_client' AND URL != 'NULL' '''
result4 = sqlContext.sql(query4)
# print("How many repositories were processed in total?")
# result4.show()


# In[42]:


query5 = """SELECT COUNT(RETRIEVAL_STAGE), DOWNLOADER_ID
            FROM GHTORRENT_RECORDS 
            WHERE RETRIEVAL_STAGE='api_client' AND URL != 'NULL'
            GROUP BY DOWNLOADER_ID
            ORDER BY COUNT(RETRIEVAL_STAGE) DESC 
            LIMIT 10"""
result5 = sqlContext.sql(query5)
# result5.show()


# In[43]:


# [(79623, '13'), (1378, '21'), (1134, '40'), (368, '18'), (357, '42'), (356, '9'), (352, '4'), (342, '25'), (333, '22'), (332, '6')]
query6 = """SELECT COUNT(RETRIEVAL_STAGE), DOWNLOADER_ID
            FROM GHTORRENT_RECORDS 
            WHERE RETRIEVAL_STAGE='api_client' AND REQUEST_STATUS='Failed'
            GROUP BY DOWNLOADER_ID
            ORDER BY COUNT(RETRIEVAL_STAGE) DESC 
            LIMIT 10"""
result6 = sqlContext.sql(query6)
# result6.show()


# In[44]:


# [(2662487, '10')]
query7 = """
            SELECT NEWT.TS
            FROM (SELECT LOGGING_LEVEL, SUBSTRING(TIMESTAMP, 12, 2) AS TS, DOWNLOADER_ID, RETRIEVAL_STAGE, REQUEST_STATUS, URL, ACCESS_KEY
                  FROM GHTORRENT_RECORDS) AS NEWT
            GROUP BY NEWT.TS
            ORDER BY COUNT(NEWT.TS) DESC
            LIMIT 1
            """
result7 = sqlContext.sql(query7)
# result7.show()


# In[46]:


# [('https://api.github.com/repos/greatfakeman/Tabchi', 79539)]
query8 = '''
        SELECT  URL
        FROM GHTORRENT_RECORDS
        WHERE URL != 'NULL'
        GROUP BY URL
        ORDER BY COUNT(URL) DESC
        LIMIT 1
        '''
result8 = sqlContext.sql(query8)
# result8.show()


# In[47]:


# [('ac6168f8776', 79623), ('46f11b5791b', 1340), ('9115020fb01', 1134), ('c1240f63b5b', 371), ('2776f3ba0a5', 368)]
query9 = '''
        SELECT  ACCESS_KEY, COUNT(ACCESS_KEY) AS key_count
        FROM GHTORRENT_RECORDS
        WHERE ACCESS_KEY != 'NULL'
        GROUP BY ACCESS_KEY
        ORDER BY key_count DESC
        LIMIT 5
        '''
result9 = sqlContext.sql(query9)
# result9.show()


# ### Table 2 Queries
# 

# In[49]:


query10 = "SELECT COUNT(*) FROM IMPORTANT_REPOS"
result10 = sqlContext.sql(query10)
# print("How many records does the IMPORTANT_REPOS table contain?")
# result10.show()


# In[50]:


query11 = '''
            SELECT COUNT(ir.URL)
            FROM GHTORRENT_RECORDS gt
            INNER JOIN IMPORTANT_REPOS ir
                ON gt.URL != 'NULL' AND gt.URL=ir.URL
            '''
result11 = sqlContext.sql(query11)
# print("How many records in the log file refer to entries in the interesting file?")
# result11.show()


# In[48]:


query12 = '''
            SELECT ir.URL, COUNT(ir.URL)
            FROM GHTORRENT_RECORDS gt
            INNER JOIN IMPORTANT_REPOS ir
            ON gt.URL != 'NULL' AND gt.URL=ir.URL AND gt.REQUEST_STATUS = 'Failed' 
            GROUP BY ir.URL
            ORDER BY COUNT(ir.URL) DESC
            LIMIT 5
            '''
result12 = sqlContext.sql(query12)
end_time = time.time()
# print("Which of the interesting repositories has the most failed API calls?")
# result12.show()
print("TimeTaken: {} ms".format(end_time-start_time))


# In[ ]:




