import csv
import gzip
import os
import sys
import time
import datetime
sys.path.append("/home/junhe/fraud_model/Code/tools/csv_operations")
import csv_ops
from csv_ops import *

from multiprocessing import Pool


def merge_all_data_sources( day_start,  n_Days):
    
    day=day_start#start date
    nDays=n_Days # number of days to process
    
    for iDay in range(nDays):
        file1="/home/junhe/fraud_model/Data/Raw_Data/signals/fraud_signal_flat_"+str(day)+".csv.gz"
        file2="/home/junhe/fraud_model/Data/Raw_Data/threatmetrix_payer_w_tmxrc/threatmetrix_payer_flat_"+str(day)+".csv.gz"
        file3="/home/junhe/fraud_model/Data/Raw_Data/threatmetrix_payee_w_tmxrc/threatmetrix_payee_flat_"+str(day)+".csv.gz"
        file_out="/home/junhe/fraud_model/Data/Raw_Data/merged_data_w_tmxrc/signals_threatmetrix_payer_payee_"+str(day)+".csv.gz"
        file_out_tmp= "/home/junhe/fraud_model/Data/Raw_Data/merged_data_w_tmxrc/merge_tmp_"+str(day)+".csv.gz"
        
        
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
    merge_all_data_sources(arg[0],arg[1])
'''
input_list = (
              [datetime.date(2014,7,1),62],
              [datetime.date(2014,9,1),61],
              [datetime.date(2014,11,1),61],
              )

pool = Pool(processes=3)
pool.map(merge_all_data_sources_helper, input_list)
'''
input_list = (
              [datetime.date(2015,1,1),31],
              [datetime.date(2015,2,1),28],
              )

pool = Pool(processes=2)
pool.map(merge_all_data_sources_helper, input_list)


