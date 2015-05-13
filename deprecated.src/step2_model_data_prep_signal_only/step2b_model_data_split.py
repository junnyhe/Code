import csv
import gzip
import os
import sys
import datetime
import random
from numpy import *
sys.path.append("/Users/junhe/Documents/workspace/fraud_model/src/csv_operations")
import csv_ops
from csv_ops import *



################################################################################
# Split ins and oos                                                            #
################################################################################
oos_frac=0.2 #fraction used for oos
work_dir='/Users/junhe/Documents/Data/Model_Data_Signal_Only/'
input_file="model_data_ds.csv.gz"

# Compute data stats 
csv_EDD(work_dir+input_file)


infile=gzip.open(work_dir+input_file,'rb')
incsv=csv.reader(infile)

header=incsv.next()
print header

main_file_name=input_file.split('.')
main_file_name=main_file_name[0]

ins_file=work_dir+main_file_name+'_ins.csv.gz'
insfile=gzip.open(ins_file,'wb')
inscsv=csv.writer(insfile)
inscsv.writerow(header)

oos_file=work_dir+main_file_name+'_oos.csv.gz'
oosfile=gzip.open(oos_file,'wb')
ooscsv=csv.writer(oosfile)
ooscsv.writerow(header)


random.seed(1)
nRow=0
for row in incsv:
    if random.random()>oos_frac:
        inscsv.writerow(row)
    else:
        ooscsv.writerow(row)
        
    nRow+=1
    if nRow%10000 ==0:
        print nRow," rows are processed"
        
print "Total, ", nRow," rows are processed;"

infile.close()


