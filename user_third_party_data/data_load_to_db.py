import gzip
import csv
import re
import json
import datetime
import itertools
from phpserialize import *
from pymongo import MongoClient

def show(cursor):
    for document in cursor:
        print(document)

def getData(cursor):
    data=[]
    for document in cursor:
        #data.append(json.loads(json.dumps(document)))
        data.append(document)
    return data
    
    
client = MongoClient()
db = client.fraud

# #db.user_third_party_data_fb.drop()
# #db.createCollection('user_third_party_data_fb')
# #db.user_third_party_data_fb.stats()

"""
################## load data ##########################
start_day=datetime.date(2015,4,1)
data_dir="/fraud_model/Data/Raw_Data/user_third_party_data/"
   
day=start_day
nDays=1
   
data=[]
nRow=0
for iDay in range(nDays):
        
    print "loading data for day: ",str(day)
        
    input_file=data_dir+"user_third_party_data_"+str(day)+".csv.gz"
    infile=gzip.open(input_file,'rb')
    incsv=csv.DictReader(infile)
        
    for row in incsv:
        if row['third_party']=='1':
            if row['data'] !='':
                tmp = row['data']
                tmp = re.sub(r'O:([0-9]*):"stdClass"', 'a', tmp)
                tmp = loads(tmp)
                row['data'] = tmp
            #data.append(row)
            result = db.user_third_party_data_fb.insert_one( json.loads(json.dumps(row)) )
            nRow+=1
                
        #if nRow>100:
        #    break
"""


'''
cursor = db.user_third_party_data_fb.distinct("group_id")
#show(cursor)
data = getData(cursor)
print "distinct group_id per day", len(data)


cursor = db.user_third_party_data_fb.find({ "data": { "$ne": None } }).distinct("group_id")
#show(cursor)
data = getData(cursor)
print "distinct group_id per day for non-empty record", len(data)

cursor = db.user_third_party_data_fb.aggregate( [
   { "$group": { "_id": "$group_id", "totalPop": { "$sum": 1 } } }
] )
#show(cursor)
data = getData(cursor)

cursor = db.user_third_party_data_fb_nonEmpty.aggregate( [
   { "$group": { "_id": "$group_id", "totalPop": { "$sum": 1 } } }
] )
#show(cursor)
data = getData(cursor)
'''

cursor = db.user_third_party_data_fb.find()
#show(cursor)
data = getData(cursor)
print "data len", len(data)


# cursor = db.user_third_party_data_fb.find()
# for document in cursor:
#     print(document)

# #cursor = db.user_third_party_data_fb.find({"address.zipcode": "10075"})
# cursor = db.user_third_party_data_fb.find({"grades.score": {"$gt": 30}})
# for document in cursor:
#     print(document)




