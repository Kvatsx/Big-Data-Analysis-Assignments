import os
from argparse import ArgumentParser
import time
# parser = ArgumentParser()
# parser.add_argument("-d", "--num_executors", dest="ne", help="number of executors")
# args = parser.parse_args()
# num_executors = args[0]
# print("hello", num_executors)
memory = '100g'
num_executors = 2
pyspark_submit_args = '--packages org.postgresql:postgresql:42.2.9 pyspark-shell' + ' --driver-memory ' + memory + ' pyspark-shell ' + '--num-executors ' + str(num_executors)

os.environ["PYSPARK_SUBMIT_ARGS"] = pyspark_submit_args

import findspark
findspark.init()
findspark.find()

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('BDA_A2').master('local').getOrCreate()


df1 = spark.read.format("jdbc").option("url", "jdbc:postgresql://localhost:5432/assignment1").option("dbtable", "GHTORRENT_RECORDS").option('user', 'postgres').option('password', 'admin').option('driver', 'org.postgresql.Driver').load()

df2 = spark.read.format("jdbc").option("url", "jdbc:postgresql://localhost:5432/assignment1").option("dbtable", "IMPORTANT_REPOS").option('user', 'postgres').option('password', 'admin').option('driver', 'org.postgresql.Driver').load()


print("Table1")
df1.printSchema()

print("Table2")
df2.printSchema()


df1.createOrReplaceTempView('GHTORRENT_RECORDS')
df2.createOrReplaceTempView('IMPORTANT_REPOS')

print("Query execution started")
start_time = time.time()

query2 = "SELECT COUNT(*) FROM GHTORRENT_RECORDS"
result2 = spark.sql(query2)
# print("How many records does the table contain?") 
# result2.show()


query3 = "SELECT COUNT(*) FROM GHTORRENT_RECORDS WHERE LOGGING_LEVEL='WARN'"
result3 = spark.sql(query3)
# print("Count the number of WARNing messages.")
# result3.show()


query4 = '''SELECT COUNT(DISTINCT URL) 
               FROM GHTORRENT_RECORDS 
               WHERE RETRIEVAL_STAGE = 'api_client' AND URL != 'NULL' '''
result4 = spark.sql(query4)
# print("How many repositories were processed in total?")
# result4.show()



query5 = """SELECT COUNT(RETRIEVAL_STAGE), DOWNLOADER_ID
            FROM GHTORRENT_RECORDS 
            WHERE RETRIEVAL_STAGE='api_client' AND URL != 'NULL'
            GROUP BY DOWNLOADER_ID
            ORDER BY COUNT(RETRIEVAL_STAGE) DESC 
            LIMIT 10"""
result5 = spark.sql(query5)
# result5.show()


query6 = """SELECT COUNT(RETRIEVAL_STAGE), DOWNLOADER_ID
            FROM GHTORRENT_RECORDS 
            WHERE RETRIEVAL_STAGE='api_client' AND REQUEST_STATUS='Failed'
            GROUP BY DOWNLOADER_ID
            ORDER BY COUNT(RETRIEVAL_STAGE) DESC 
            LIMIT 10"""
result6 = spark.sql(query6)
# result6.show()


query7 = """
            SELECT NEWT.TS
            FROM (SELECT LOGGING_LEVEL, SUBSTRING(TIMESTAMP, 12, 2) AS TS, DOWNLOADER_ID, RETRIEVAL_STAGE, REQUEST_STATUS, URL, ACCESS_KEY
                  FROM GHTORRENT_RECORDS) AS NEWT
            GROUP BY NEWT.TS
            ORDER BY COUNT(NEWT.TS) DESC
            LIMIT 1
            """
result7 = spark.sql(query7)
# result7.show()


query8 = '''
        SELECT  URL
        FROM GHTORRENT_RECORDS
        WHERE URL != 'NULL'
        GROUP BY URL
        ORDER BY COUNT(URL) DESC
        LIMIT 1'''
result8 = spark.sql(query8)
# result8.show()


query9 = '''
        SELECT  ACCESS_KEY, COUNT(ACCESS_KEY) AS key_count
        FROM GHTORRENT_RECORDS
        WHERE ACCESS_KEY != 'NULL'
        GROUP BY ACCESS_KEY
        ORDER BY key_count DESC
        LIMIT 5'''
result9 = spark.sql(query9)
# result9.show()




query10 = "SELECT COUNT(*) FROM IMPORTANT_REPOS"
result10 = spark.sql(query10)
# print("How many records does the IMPORTANT_REPOS table contain?")
# result10.show()



query11 = '''
            SELECT COUNT(ir.URL)
            FROM GHTORRENT_RECORDS gt
            INNER JOIN IMPORTANT_REPOS ir
                ON gt.URL != 'NULL' AND gt.URL=ir.URL
            '''
result11 = spark.sql(query11)
# print("How many records in the log file refer to entries in the interesting file?")
# result11.show()


query12 = '''
            SELECT ir.URL, COUNT(ir.URL)
            FROM GHTORRENT_RECORDS gt
            INNER JOIN IMPORTANT_REPOS ir
            ON gt.URL != 'NULL' AND gt.URL=ir.URL AND gt.REQUEST_STATUS = 'Failed' 
            GROUP BY ir.URL
            ORDER BY COUNT(ir.URL) DESC
            LIMIT 5'''
result12 = spark.sql(query12)
# print("Which of the interesting repositories has the most failed API calls?")
# result12.show()


end_time = time.time()

print("Time Taken with {} executors: {} ms".format(num_executors, end_time-start_time))

print("Query execution ended")
