import csv
import gzip
import os
import sys
import time
import datetime
sys.path.append("/fraud_model/Code/tools/csv_operations")
import csv_ops
from csv_ops import *

from multiprocessing import Pool

global work_dir
work_dir="/fraud_model/Data/Raw_Data/merged_data_w_tmxrc/"

def merge_all_data_sources( day_start,  n_Days):
    
    day=day_start#start date
    nDays=n_Days # number of days to process
    
    for iDay in range(nDays):
        file1="/fraud_model/Data/Raw_Data/signals/fraud_signal_flat_"+str(day)+".csv.gz"
        file2="/fraud_model/Data/Raw_Data/threatmetrix_payer_w_tmxrc/threatmetrix_payer_flat_"+str(day)+".csv.gz"
        file3="/fraud_model/Data/Raw_Data/threatmetrix_payee_w_tmxrc/threatmetrix_payee_flat_"+str(day)+".csv.gz"
        file_out=work_dir+"signals_threatmetrix_payer_payee_"+str(day)+".csv.gz"
        file_out_tmp= work_dir+"merge_tmp_"+str(day)+".csv.gz"
        
        
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
        cmdout=os.system('rm '+file_out_tmp.replace(" ","\ "))
        
        #increment day by one
        day = day+datetime.timedelta(1)
        
        
def merge_all_data_sources_helper(arg):
    merge_all_data_sources(arg,1)

# last day of the perirod
if len(sys.argv) <=1: # if last day is not specified by stdin
    year=2015
    month=4
    day=30
    nDays = 30
else:
    year=int(sys.argv[1])
    month=int(sys.argv[2])
    day=int(sys.argv[3])
    nDays=int(sys.argv[4])

print "first day to merge:",year,'-',month,'-',day
nWorkers = 4
dayEnd = datetime.date(year, month, day)

# prepare datelist to roll up, skip dates that already have been rolled up
dateList = []
for i in range(nDays):
    dayToProcess=dayEnd-datetime.timedelta(i)
    if os.path.exists(work_dir + "signals_threatmetrix_payer_payee_"+str(dayToProcess)+".csv.gz"):
        print "signals_threatmetrix_payer_payee_"+str(dayToProcess)+".csv.gz"," already exits, skipping ..."
    else:
        print "signals_threatmetrix_payer_payee_"+str(dayToProcess)+".csv.gz"," rollup will be processed"
        dateList.append(dayToProcess)



pool = Pool(processes=nWorkers)
pool.map(merge_all_data_sources_helper, dateList)


