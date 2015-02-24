import csv
import gzip
import os
import sys
import datetime
import random
from numpy import *
sys.path.append("/Users/junhe/Documents/workspace/fraud_model/src/csv_operations")

infile=gzip.open('/Users/junhe/Documents/Data/Model_Data_Signal_Tmx/model_data_ds_ins.csv.gz','rb')
incsv=csv.DictReader(infile)

outfile=open('/Users/junhe/Documents/Results/adhoc/model_data_ds_train_bads.csv','w')
outcsv=csv.writer(outfile)

header=incsv.fieldnames
outcsv.writerow(header)

for row in incsv:
    if row['target']=='1':
        outcsv.writerow([row[var] for var in header])
        
        
        