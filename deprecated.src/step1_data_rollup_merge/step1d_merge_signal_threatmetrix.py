import csv
import gzip
import os
import sys
import time
import datetime
sys.path.append("/Users/junhe/Documents/workspace/fraud_model/src/model_tools")
sys.path.append("/Users/junhe/Documents/workspace/fraud_model/src/csv_operations")
import csv_ops
from csv_ops import *

day=datetime.date(2014,11,5) #start date
nDays=27 # number of days to process

for iDay in range(nDays):
    file1="/Users/junhe/Documents/Data/Raw_Data/signals/fraud_signal_flat_"+str(day)+".csv.gz"
    file2="/Users/junhe/Documents/Data/Raw_Data/threatmetrix_payer/threatmetrix_payer_flat_"+str(day)+".csv.gz"
    file3="/Users/junhe/Documents/Data/Raw_Data/threatmetrix_payee/threatmetrix_payee_flat_"+str(day)+".csv.gz"
    file_out="/Users/junhe/Documents/Data/Raw_Data/merged_data/signals_threatmetrix_payer_payee_"+str(day)+".csv.gz"
    file_out_tmp= "/Users/junhe/Documents/Data/Raw_Data/merged_data/merge_tmp_"+str(day)+".csv.gz"
    
    key_list=['payment_request_id']
    
    # merge signal and payer threatmetrix
    t0=time.time()
    print "Merging signal and payer threatmetrix for "+str(day)
    csv_merge(file1, file2, key_list, file_out_tmp)
    print "Merging signal and payer threatmetrix done; time lapsed: ",time.time()-t0,'sec'
    
    # merge above results wtih payee threatmetrix
    print "Merge all three data sources for "+str(day)
    csv_merge(file_out_tmp, file3, key_list, file_out)
    print "Merge all three data sources done ; total time lapsed: ",time.time()-t0,'sec'
    
    #delete intermediate file
    cmdout=os.system('rm '+file_out_tmp)
    
    #increment day by one
    day = day+datetime.timedelta(1)