import gzip
import csv
import random
import sys
sys.path.append("/fraud_model/Code/tools/model_tools")
from load_data import *
from numpy import *

out_dir='/fraud_model/Data/Model_Data_Signal_Tmx_v3wd_newest_time/'

input_file = out_dir+"model_data_wd_oos.csv.gz"
output_file = "/fraud_model/Data/Model_Data_Signal_Tmx_v3wd_newest_time/model_data_wd_oos_ds.csv.gz"
fin = gzip.open(output_file,'rb')
fin_csv = csv.DictReader(fin)

nRow=0
nTag=0
nTgt=0
for row in fin_csv:

    nRow+=1
    if row['target']=='1':
        nTgt+=1
    if row['manual_review']=='1':
        nTag+=1
    
    
print nRow,nTgt,nTag    