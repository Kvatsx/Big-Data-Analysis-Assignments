'''
------------------------------------------------------
@Author: Kaustav Vats (kaustav16048@iiitd.ac.in), Abhishek Agarwal ()
@Roll-No: 2016048, 2016126
------------------------------------------------------
'''

import psycopg2

conn = psycopg2.connect(database="Assignment1", user = "postgres", password = "admin", host = "127.0.0.1", port = "5432")
print("Opened database successfully")

cur = conn.cursor()
Table = '''CREATE TABLE GHTORRENT
      ( ID                  INT     PRIMARY KEY     NOT NULL,
        LOGGING_LEVEL       TEXT    NOT NULL,
        TIMESTAMP           TEXT    NOT NULL,
        DOWNLOADER_ID       INT     NOT NULL,
        RETRIEVAL_STATUS    TEXT    NOT NULL,
        OPERATION           TEXT    NOT NULL);'''

cur.execute()
print "Table created successfully"

conn.commit()
conn.close()
