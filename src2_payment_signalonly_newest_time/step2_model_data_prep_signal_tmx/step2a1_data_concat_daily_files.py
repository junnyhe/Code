import csv
import gzip
import os
import datetime
import random
import sys
from multiprocessing import Pool

global signal_dir, tgt_dir, rule_dir, out_dir


# get input
signal_dir = "/fraud_model/Data/Raw_Data/signals/"
tgt_dir = "/fraud_model/Data/Raw_Data/targets/"
rule_dir = "/fraud_model/Data/Raw_Data/rule_results_pmt_direction/"
out_dir='/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt_signalonly_newest_time/'

    
def cat_daily_files(start_day,nDays,out_file_name):
    
    
    # load target to dict
    day=start_day
    target={}
    nRow=0
    for iDay in range(nDays):
        
        print "loading targets for day: ",str(day)
        
        input_file=tgt_dir+"fraud_target_"+str(day)+".csv.gz"
        infile=gzip.open(input_file,'rb')
        incsv=csv.DictReader(infile)
        
        for row in incsv:
            target[row['payment_request_id']]=row
            
        
        #increment day by one
        day = day+datetime.timedelta(1)
        infile.close()
        
    print "Totally, number of targets:",len(target.keys())
    
    
    # load rule results and payment direction to dict
    day=start_day
    rule_direction={}
    nRow=0
    for iDay in range(nDays):
        
        print "loading rule results and payment direction for day: ",str(day)
        
        input_file=rule_dir+"fraud_rule_results_pmt_direction_"+str(day)+".csv.gz"
        infile=gzip.open(input_file,'rb')
        incsv=csv.DictReader(infile)
        
        for row in incsv:
            rule_direction[row['payment_request_id']]=row
            
        
        #increment day by one
        day = day+datetime.timedelta(1)
        infile.close()
        
        
    
    # get input signal data header
    sig_file=signal_dir+"fraud_signal_flat_"+str(start_day)+".csv.gz"
    sigfile=gzip.open(sig_file,'rb')
    sigcsv=csv.DictReader(sigfile)
    header_signals=sigcsv.fieldnames
    print header_signals
    
    # define output file
    header_out=header_signals+['target','blacklist_reason','target2','manual_review', 'direction']
    
    output_file_1=out_dir+out_file_name+"_pmt.csv.gz"
    outfile_1=gzip.open(output_file_1,'w')
    outcsv_1=csv.writer(outfile_1)
    outcsv_1.writerow(header_out)
    
    output_file_2=out_dir+out_file_name+"_wd.csv.gz"
    outfile_2=gzip.open(output_file_2,'w')
    outcsv_2=csv.writer(outfile_2)
    outcsv_2.writerow(header_out)
    
    
    day=start_day
    for iDay in range(nDays):
        
        print "attaching targets and rule results for day: ",str(day)
        
        input_file=signal_dir+"fraud_signal_flat_"+str(day)+".csv.gz"
        infile=gzip.open(input_file,'rb')
        incsv=csv.DictReader(infile)
        print input_file
        
        for row in incsv:
            row['target']=0
            row['blacklist_reason']=''
            row['target2']=0
            row['manual_review']=0
            if row['payment_request_id'] in target:
                row['target']=1
                row['blacklist_reason']=target[row['payment_request_id']]['blacklist_reason']
                if row['blacklist_reason'] == 'Fraud':
                    row['target2']=1
            
            if rule_direction[row['payment_request_id']]['action']=='2':
                #print "Manual review case found"
                row['manual_review']=1
            
            row['direction']=rule_direction[row['payment_request_id']]['direction']
            
            
            
            if row['fs_payment_request_id'] !='' : # only output to model data if pr exist in fraud_signals table
                # ouput to payment file
                if row['direction']=='1':
                    outcsv_1.writerow([str(row[var]).replace(',',';') for var in header_out])
                    #outcsv_1.writerow([row[var] for var in header_out])
            
                # output to withdrawal file
                elif row['direction']=='2': 
                    outcsv_2.writerow([str(row[var]).replace(',',';')  for var in header_out])
                    #outcsv_2.writerow([row[var]  for var in header_out])
                
        # increment day by one
        day = day+datetime.timedelta(1)
        infile.close()
        
    outfile_1.close()
    outfile_2.close()


def cat_daily_files_helper(arg):
    start_day=arg[0] #start date
    nDays=arg[1] # number of days to process
    out_file_name=arg[2]
    cat_daily_files(start_day,nDays,out_file_name)




# last day the model will be trained on
if len(sys.argv) <=1: # if last day is not specified by stdin
    year=2015
    month=3
    day=31
else:
    year=int(sys.argv[1])
    month=int(sys.argv[2])
    day=int(sys.argv[3])

print "Last day to train model:",year,'-',month,'-',day

last_day=datetime.date(year,month,day)


# prepare input list
input_list = []

# prepare training data with last two months
start_day=last_day-datetime.timedelta(60-1) #start date
nDays=60 # number of days to process
out_file_name='model_data'
input_list.append([start_day,nDays,out_file_name])

# prepare test data 6 months before training data
for i in range(1,7):
    start_day=last_day-datetime.timedelta(60+30*i-1) #start date
    nDays=30 # number of days to process
    out_file_name='test_data_'+str(i)+'mo'
    input_list.append([start_day,nDays,out_file_name])


pool = Pool(processes=4)
pool.map(cat_daily_files_helper, input_list)




# additional test data
start_day=datetime.date(2015,4,1) #start date
nDays=30 # number of days to process
out_file_name='test_data_-1mo'
cat_daily_files(start_day,nDays,out_file_name)
