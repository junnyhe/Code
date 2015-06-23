import csv
import gzip
import os
import datetime
import time
import random
import sys
import json
import re
import itertools
from phpserialize import *
from multiprocessing import Pool

start_day=datetime.date(2015,4,1)
data_dir="/fraud_model/Data/Raw_Data/user_third_party_data/"

day=start_day
nDays=1

data=[]
nRow=0
for iDay in range(nDays):
    
    t0=time.time()
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
            data.append(row)
            nRow+=1
            
            #if nRow>10000:
            #    break
            
            if nRow % 10000 ==0:
                print nRow,"loaded ..."
    
    #increment day by one
    day = day+datetime.timedelta(1)
    infile.close()
    
    print "load data for day ",str(day),"takes:",time.time()-t0,"secs"
    print "number of rows before deduping for ",str(day),"is:",nRow

data_dict={}
for row in data:
    if row['group_id'] not in data_dict:
        data_dict[row['group_id']]=row
    else:
        if data_dict[row['group_id']]['data'] == None:
            data_dict[row['group_id']]=row
        else:
            if row['data'] !=None:
                if len(row['data'])>len(data_dict[row['group_id']]['data']):
                    data_dict[row['group_id']]=row


data_dedup = [data_dict[key] for key in data_dict]
print "number of rows after deduping for ",str(day),"is:",len(data_dedup)
del data_dict 


data_dedup_nempty=[ele['data'] for ele in data_dedup if  ele['data'] !=None]
print "number of rows for deduped non-empty data",str(day),"is:",len(data_dedup_nempty)

    
'''
data_fb=[ele['data'] for ele in data if  ele['data'] !=None]
data_keys = [ele.keys() for ele in data_fb]
data_keys = list(itertools.chain(*data_keys))
data_keys = list(set(data_keys))

data_fb_col = {}
for key in data_keys:
    data_fb_col[key]=[]
    
for row in data_fb:
    for key in data_keys:
        if key in row:
            data_fb_col[key].append(row[key])
        else:
            data_fb_col[key].append(None)

# 
# ['feed',
#  'picture',
#  'verified',
#  'name',
#  'gender',
#  'posts',
#  'locations',
#  'updated_time',
#  'photos',
#  'birthday',
#  'likes',
#  'timezone',
#  'friends',
#  'id']

    
data_fb_col['feed'][:20]
data_fb_col['picture'][:20]
data_fb_col['verified'][:20]
data_fb_col['name'][:20]
data_fb_col['gender'][:20]
data_fb_col['posts'][:20]
data_fb_col['locations'][:20]
data_fb_col['updated_time'][:20]
data_fb_col['photos'][:20]
data_fb_col['birthday'][:20]
data_fb_col['likes'][:20]
data_fb_col['timezone'][:20]
data_fb_col['friends'][:20]
data_fb_col['id'][:20]

#data_fb_col['posts'][11]['data'][2].keys()
#data_fb_col['posts'][11]['data'][2]['likes']
#data_fb_col['posts'][11]['data'][2]['likes']['data'] 
#data_fb_col['locations'][11]['data'][8]
nLocations=len(data_fb_col['locations'][11]['data'])
data_fb_col['photos'][11]['data'][8]
nPhotos=len(data_fb_col['photos'][11]['data'])
len(data_fb_col['likes'][11]['data'])

nFriends=len(data_fb_col['friends'][11])

'''
    