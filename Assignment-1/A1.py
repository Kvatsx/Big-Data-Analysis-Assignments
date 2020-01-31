#!/usr/bin/env python
# coding: utf-8

# In[45]:


'''
----------------------------------------------------------
@Authors: 
[*] Kaustav Vats (kaustav16048@iiitd.ac.in), 2016048
[*] Abhishek Agarwal (abhishek16126@iiitd.ac.in), 2016126
----------------------------------------------------------
'''
import psycopg2
import csv
from tqdm import tqdm_notebook as tqdm
import time


# ### Load data and Store data

# In[31]:


Path = "Data/"
FileName = "ghtorrent-logs.txt"
def ReadData():
    f = open(Path+FileName, 'r', encoding='utf8')
    return PreProcess(f.readlines())

def PreProcess(data):
    D = []
    for line in tqdm(data):
        line = line.strip()
        line = line.replace("ghtorrent-", "")
        line = line.replace(" -- ", ", ")
        line = line.replace(".rb:", ",")
        temp = line.split(", ", 4)
        
        if temp[0] == 'E' or len(temp) != 5:
            continue
        
        misc = temp[-1]
        line = temp[:4]
        
        temp2 = []
        if line[3] == 'api_client':
            t_status = misc.split()[0]
            temp2.append(t_status)
            
            if t_status == "Successful":
                # Extracting URL
                idx = misc.find("URL:")
                t_misc = misc[idx+5:]
                t_url = t_misc.split("/")[:6]
                if t_url[-1].find("?") != -1:
                    t_url[-1] = t_url[-1].split("?")[0]
                t_url = "/".join(t_url)
                
                temp2.append(t_url.split(",")[0])
                
                temp2.append("NULL")
            elif t_status == "Failed":
                # Extracting URL
                idx = misc.find("URL:")
                t_misc = misc[idx+5:]
                t_url = t_misc.split("/")[:6]
                if t_url[-1].find("?") != -1:
                    t_url[-1] = t_url[-1].split("?")[0]
                t_url = "/".join(t_url)
                temp2.append(t_url.split(",")[0])
                
                # Extracting Access Key
                idx = misc.find("Access:")
                t_misc = misc[idx+8:]
                idx = t_misc.find(",")
                temp2.append(t_misc[:idx])
            
            else:
                temp2.append("NULL")
                temp2.append("NULL")
        
        else:
            # Request Status
            temp2.append("NULL")
            
            # Extracting URL
            if misc.find("/") != -1 and misc.find("User with email") == -1:
                t_misc = misc.split("/")
                if len(t_misc) > 3:
                    temp2.append("NULL")
                else:
                    left_misc, right_misc = t_misc[0].split()[-1], t_misc[1].split()[0]
                    right_misc = right_misc.split("->")[0].split(" ")[0]
                    temp2.append("https://api.github.com/repos/"+left_misc+"/"+right_misc)

            else:
                temp2.append("NULL")

            # Extracting Access Key
            temp2.append("NULL")
            
        line = line+temp2
        D.append(line)
        
    return D   


# In[44]:


Data = ReadData()
print(Data[:5])


# In[4]:


for line in Data:
    if (len(line) != 7):
        print(line)


# In[14]:


f = open(Path+'NewData.csv', 'w', newline='', encoding='utf8')
writer = csv.writer(f)
writer.writerows(Data)
f.close()


# In[6]:


size = []
for d in Data:
    size.append(len(d))
    if (len(d) < 5):
        print(d)
size = set(size)
print(size)
print(Data[:3])


# In[46]:


DATABASE_NAME = "assignment1"
USER = "postgres"
PASSWORD = "admin"
HOST_IP = "127.0.0.1"
PORT = "5432"


# In[55]:


conn = psycopg2.connect(database=DATABASE_NAME, user = USER, password = PASSWORD, host = HOST_IP, port = PORT)
cur = conn.cursor()


# ### Insert Data in Postgresql

# In[47]:


cur = conn.cursor()
GHTORRENT = '''CREATE TABLE IF NOT EXISTS GHTORRENT_RECORDS
       (LOGGING_LEVEL       TEXT    NOT NULL,
        TIMESTAMP           TEXT    NOT NULL,
        DOWNLOADER_ID       TEXT    NOT NULL,
        RETRIEVAL_STAGE     TEXT    NOT NULL,
        REQUEST_STATUS      TEXT    NOT NULL,
        URL                 TEXT    NOT NULL,
        ACCESS_KEY          TEXT    NOT NULL);'''
cur.execute('''DROP TABLE IF EXISTS GHTORRENT_RECORDS;''')
cur.execute(GHTORRENT)
# temp = [['INFO', '2017-03-22T20:11:49+00:00', '31', 'ghtorrent', 'Added pullreq_commit 244eeac28bf419642d5d5c3b388bd2999c8c72e6 to tgstation/tgstation -> 25341'], ['DEBUG', '2017-03-23T11:15:14+00:00', '30', 'retriever', 'Commit mzvast/FlappyFrog -> 80bf5c5fde7be6274a2721422f4d9a773583f73c exists'], ['DEBUG', '2017-03-22T20:15:48+00:00', '35', 'ghtorrent', 'Parent af8451e16e077f7c6cae3f98bf43bffaca562f88 for commit 2ef393531a3cfbecc69f17d2cedcc95662fae1e6 exists']]
sql = '''INSERT INTO GHTORRENT_RECORDS
        (LOGGING_LEVEL, TIMESTAMP, DOWNLOADER_ID, RETRIEVAL_STAGE, REQUEST_STATUS, URL, ACCESS_KEY) 
        VALUES (%s, %s, %s, %s, %s, %s, %s);'''

cur.executemany(sql, Data)
conn.commit()
# f = open(Path+'NewData.csv', 'r', encoding='utf8')
# cur.copy_from(f, 'GHTORRENT', sep=",")
# f.close()
# conn.close()


# ### How many records does the table contain?

# In[16]:


query2 = "SELECT COUNT(*) FROM GHTORRENT_RECORDS"
cur.execute(query2)
num_records = cur.fetchall()
print("How many records does the table contain?", num_records[0][0])


# ### Count the number of WARNing messages.

# In[5]:


query3 = "SELECT COUNT(*) FROM GHTORRENT_RECORDS WHERE LOGGING_LEVEL='WARN';"
cur.execute(query3)
rows = cur.fetchall()
print("Count the number of WARNing messages.", rows[0][0])


# ### How many repositories were processed in total?

# In[6]:


# 78588
query4 = '''SELECT COUNT(DISTINCT URL) 
               FROM GHTORRENT_RECORDS 
               WHERE RETRIEVAL_STAGE = 'api_client' AND URL != 'NULL';'''
cur.execute(query4)
rows = cur.fetchall()
print("How many repositories were processed in total?", rows[0][0])


# ### Which 10 clients did the highest HTTP requests?

# In[7]:


query5 = """SELECT COUNT(RETRIEVAL_STAGE), DOWNLOADER_ID
            FROM GHTORRENT_RECORDS 
            WHERE RETRIEVAL_STAGE='api_client' AND URL != 'NULL'
            GROUP BY DOWNLOADER_ID
            ORDER BY COUNT(RETRIEVAL_STAGE) DESC 
            LIMIT 10"""
cur.execute(query5)
rows = cur.fetchall()
print(rows)


# ### Which 10 client did the highest FAILED HTTP requests?

# In[8]:


# [(79623, '13'), (1378, '21'), (1134, '40'), (368, '18'), (357, '42'), (356, '9'), (352, '4'), (342, '25'), (333, '22'), (332, '6')]
query6 = """SELECT COUNT(RETRIEVAL_STAGE), DOWNLOADER_ID
            FROM GHTORRENT_RECORDS 
            WHERE RETRIEVAL_STAGE='api_client' AND REQUEST_STATUS='Failed'
            GROUP BY DOWNLOADER_ID
            ORDER BY COUNT(RETRIEVAL_STAGE) DESC 
            LIMIT 10"""
cur.execute(query6)
rows = cur.fetchall()
print(rows)


# ### What is the most active hour of day?

# In[9]:


# [(2662487, '10')]
query7 = """
            SELECT NEWT.TS
            FROM (SELECT LOGGING_LEVEL, SUBSTRING(TIMESTAMP, 12, 2) AS TS, DOWNLOADER_ID, RETRIEVAL_STAGE, REQUEST_STATUS, URL, ACCESS_KEY
                  FROM GHTORRENT_RECORDS) AS NEWT
            GROUP BY NEWT.TS
            ORDER BY COUNT(NEWT.TS) DESC
            LIMIT 1
            """
cur.execute(query7)
rows = cur.fetchall()
print(rows)


# ### What is the most active repository?

# In[17]:


# [('https://api.github.com/repos/greatfakeman/Tabchi', 79539)]
query8 = '''
        SELECT  URL
        FROM GHTORRENT_RECORDS
        WHERE URL != 'NULL'
        GROUP BY URL
        ORDER BY COUNT(URL) DESC
        LIMIT 1;
        '''
cur.execute(query8)
rows = cur.fetchall()
print(rows)


# ### Which access keys are failing most often?

# In[18]:


# [('ac6168f8776', 79623), ('46f11b5791b', 1340), ('9115020fb01', 1134), ('c1240f63b5b', 371), ('2776f3ba0a5', 368)]
query9 = '''
        SELECT  ACCESS_KEY, COUNT(ACCESS_KEY) AS key_count
        FROM GHTORRENT_RECORDS
        WHERE ACCESS_KEY != 'NULL'
        GROUP BY ACCESS_KEY
        ORDER BY key_count DESC
        LIMIT 5;
        '''
cur.execute(query9)
rows = cur.fetchall()
print(rows)


# ### Compute the number of different repositories accessed by the client ghtorrent-22 . Note the time taken by your query.

# In[25]:


indexing = """CREATE INDEX downloader_index ON GHTORRENT_RECORDS(DOWNLOADER_ID);"""
cur.execute(indexing)
conn.commit()


# In[26]:


query10 = '''
            SELECT COUNT(DISTINCT URL) 
            FROM GHTORRENT_RECORDS 
            WHERE DOWNLOADER_ID = '22' AND URL != 'NULL';
            '''

start_time = time.time()
cur.execute(query10)
end_time = time.time()

rows = cur.fetchall()
print("TimeTaken: {} ms".format(end_time-start_time))
print(rows[0][0])


# ### Now drop your index and compute the number of different repositories accessed by the client ghtorrent-22. Note the time taken by your query now.

# In[27]:


# drop_indexing = """
#                 ALTER TABLE GHTORRENT
#                 DROP INDEX GHTORRENT(DOWNLOADER_ID).downloader_index;"""
drop_indexing = '''DROP INDEX DOWNLOADER_INDEX ;'''
cur.execute(drop_indexing)
conn.commit()


# In[28]:


query11 = '''
            SELECT COUNT(DISTINCT URL) 
            FROM GHTORRENT_RECORDS 
            WHERE DOWNLOADER_ID = '22' AND URL != 'NULL';
            '''

start_time = time.time()
cur.execute(query11)
end_time = time.time()

rows = cur.fetchall()
print("TimeTaken: {} ms".format(end_time-start_time))
print(rows)


# ### Joining

# In[38]:


f = open(Path+"important-repos.csv", 'r', encoding='utf8')
data2 = csv.reader(f)
D2 = []
for row in data2:
    D2.append(row)
D2.pop(0)
print("No of Records:", len(D2))


# In[39]:


conn = psycopg2.connect(database=DATABASE_NAME, user = USER, password = PASSWORD, host = HOST_IP, port = PORT)
print("Opened database successfully")
cur = conn.cursor()
cur.execute('''DROP TABLE IF EXISTS IMPORTANT_REPOS;''')
IMPORTANT_REPOS = '''CREATE TABLE IF NOT EXISTS IMPORTANT_REPOS
       (ID          TEXT    NOT NULL,
        URL         TEXT    NOT NULL,
        OWNER_ID    TEXT    NOT NULL,
        NAME        TEXT    NOT NULL,
        LANGUAGE    TEXT    NOT NULL,
        CREATED_AT  TEXT    NOT NULL,
        FORKED_FROM TEXT    NOT NULL,
        DELETED     TEXT    NOT NULL,
        UPDATED_AT  TEXT    NOT NULL);'''
# cur.execute('''DROP TABLE IMPORTANT_REPOS;''')
cur.execute(IMPORTANT_REPOS)

sql = '''INSERT INTO IMPORTANT_REPOS
        (ID, URL, OWNER_ID, NAME, LANGUAGE, CREATED_AT, FORKED_FROM, DELETED, UPDATED_AT) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);'''

cur.executemany(sql, D2)
conn.commit()


# ### Read in the CSV file into another table call it interesting. How many records are there?

# In[40]:



cur.execute("SELECT COUNT(*) FROM IMPORTANT_REPOS")
rows = cur.fetchall()
print("How many records does the IMPORTANT_REPOS table contain?", rows)


# ### How many records in the log file refer to entries in the interesting file?

# In[52]:


# cur = conn.cursor()
query13 = '''
            SELECT COUNT(ir.URL)
            FROM GHTORRENT_RECORDS gt
            INNER JOIN IMPORTANT_REPOS ir
                ON gt.URL != 'NULL' AND gt.URL=ir.URL;
            '''
cur.execute(query13)
rows = cur.fetchall()
print("How many records in the log file refer to entries in the interesting file?", rows)


# ### Which of the interesting repositories has the most failed API calls?

# In[57]:


query14 = '''
            SELECT ir.URL, COUNT(ir.URL)
            FROM GHTORRENT_RECORDS gt
            INNER JOIN IMPORTANT_REPOS ir
            ON gt.URL != 'NULL' AND gt.URL=ir.URL AND gt.REQUEST_STATUS = 'Failed' 
            GROUP BY ir.URL
            ORDER BY COUNT(ir.URL) DESC
            LIMIT 5;
            '''
cur.execute(query14)
rows = cur.fetchall()
print("Which of the interesting repositories has the most failed API calls?", rows)


# In[ ]:




