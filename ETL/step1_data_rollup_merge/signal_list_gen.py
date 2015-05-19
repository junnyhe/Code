import csv
import gzip
import os
import datetime
import copy as cp
from numpy import *


dayEnd = datetime.date(2015,3,31)
nDays = 60
out_file_name = "/fraud_model/Data/Raw_Data/signals/signal_list_febmar.csv"
out_file = open(out_file_name,'w')
outcsv = csv.writer(out_file)
outcsv.writerow(['signal_id'])

def find_signal_list():
    
    # sample files to find signal number in date range
    print "Sample files to find signal numbers in date range"
    signal_set=set([])
    day=dayEnd
    for iDay in range(nDays): 
        print "sample day:",str(day)
        input_file="/fraud_model/Data/Raw_Data/signals/fraud_signal_"+str(day)+".csv.gz"
        infile=gzip.open(input_file,'rb')
        incsv=csv.DictReader(infile)
        
        nRow=0
        for row in incsv:
            try:
                signal_set.add(int(row['signal_id']))
            except:
                continue
            nRow+=1
            if nRow>100000:
                break
        
        #increment day by one
        day = day+datetime.timedelta(-1)
    
    signal_list = sorted(list(signal_set))
    
    print signal_list
    print "number of signals:",len(signal_set)
    
    # write signal list to disk
    for signal in signal_list:
        outcsv.writerow([signal])


find_signal_list()



