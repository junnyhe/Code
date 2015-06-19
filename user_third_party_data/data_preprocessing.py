import csv
import gzip
import os
import datetime
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
            
        if nRow>10000:
            break
        
    
    #increment day by one
    day = day+datetime.timedelta(1)
    infile.close()
    