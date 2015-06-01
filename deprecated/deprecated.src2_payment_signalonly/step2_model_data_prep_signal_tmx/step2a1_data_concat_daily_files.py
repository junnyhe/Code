import csv
import gzip
import os
import datetime
import random


def cat_daily_files(start_day,out_file_name):
    
    # get input
    signal_dir = "/fraud_model/Data/Raw_Data/signals/"
    tgt_dir = "/fraud_model/Data/Raw_Data/targets/"
    rule_dir = "/fraud_model/Data/Raw_Data/rule_results_pmt_direction/"
    out_dir = "/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt_signalonly/"
    
    
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
    
'''
# model data july/aug
start_day=datetime.date(2014,7,1) #start date
nDays=62 # number of days to process
out_file_name='model_data'
cat_daily_files(start_day,out_file_name)


# test data sept
start_day=datetime.date(2014,9,1) #start date
nDays=30 # number of days to process
out_file_name='test_data_sept'
cat_daily_files(start_day,out_file_name)


# test data oct
start_day=datetime.date(2014,10,1) #start date
nDays=31 # number of days to process
out_file_name='test_data_oct'
cat_daily_files(start_day,out_file_name)


# test data nov
start_day=datetime.date(2014,11,1) #start date
nDays=30 # number of days to process
out_file_name='test_data_nov'
cat_daily_files(start_day,out_file_name)
 

# test data dec
start_day=datetime.date(2014,12,1) #start date
nDays=31 # number of days to process
out_file_name='test_data_dec'
cat_daily_files(start_day,out_file_name)
'''

# model data july/aug
start_day=datetime.date(2014,12,1) #start date
nDays=62 # number of days to process
out_file_name='newest_model_data'
cat_daily_files(start_day,out_file_name)
