import gzip
import csv
import re
import json
import datetime
import itertools
from phpserialize import *
from pymongo import MongoClient

client = MongoClient()
db = client.fraud

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
                row['data'] = json.dumps(tmp)
            data.append(row)
            nRow+=1
            
            print row
            
        if nRow>100:
            break
        
# #cursor = db.user_thrid_party_data.find({"address.zipcode": "10075"})
# cursor = db.user_thrid_party_data.find({"grades.score": {"$gt": 30}})
# for document in cursor:
#     print(document)




