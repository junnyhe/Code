import csv
import gzip
import os
import datetime
import random

signal_dir = "/Users/junhe/Documents/Data/Raw_Data/signals/"
tgt_dir = "/Users/junhe/Documents/Data/Raw_Data/targets/"
out_dir = "/Users/junhe/Documents/Data/Model_Data_Signal_Only/"

start_day=datetime.date(2014,7,1) #start date
nDays=62 # number of days to process


header_target = [ 'payment_request_id',
    'payer_account_id', 
    'payee_account_id', 
    'app_account_id',
    'amount',
    'fee',
    'app_fee',
    'payment_type',
    'finish_time',
    'create_time', 
    'modify_time', 
    'account_id', 
    'key', 
    'value', 
    'create_time_ap',
    'modify_time_ap']



day=start_day
target={}
nRow=0
for iDay in range(nDays):
    
    print "loading targets for day: ",str(day)
    
    input_file=tgt_dir+"fraud_target_"+str(day)+".csv.gz"
    infile=gzip.open(input_file,'rb')
    incsv=csv.DictReader(infile,fieldnames=header_target)
    
    for row in incsv:
        target[row['payment_request_id']]=1
        
    
    #increment day by one
    day = day+datetime.timedelta(1)
    infile.close()
    
print "Totally, number of targets:",len(target.keys())


day=start_day

#get input signal data header
sig_file=signal_dir+"fraud_signal_flat_"+str(day)+".csv.gz"
sigfile=gzip.open(sig_file,'rb')
sigcsv=csv.DictReader(sigfile)
header_signals=sigcsv.fieldnames
print header_signals

#define output file
header_out=header_signals+['target']
output_file=out_dir+"model_data.csv.gz"
outfile=gzip.open(output_file,'w')
outcsv=csv.writer(outfile)
outcsv.writerow(header_out)

for iDay in range(nDays):
    
    print "attaching targets for day: ",str(day)
    
    input_file=signal_dir+"fraud_signal_flat_"+str(day)+".csv.gz"
    infile=gzip.open(input_file,'rb')
    incsv=csv.DictReader(infile)
    
    for row in incsv:
        if row['payment_request_id'] in target:
            row['target']=1
        else:
            row['target']=0
        if row['fs_payment_request_id'] !='': # only output to model data if pr exist in fraud_signals table
            outcsv.writerow([row[var] for var in header_out])
        #else:
        #    print "row skipped, payment state=",row['state']
        
    #increment day by one
    day = day+datetime.timedelta(1)
    infile.close()
    
outfile.close()


#down sample non-target
nontarget_samp_rate=0.05

input_file=out_dir+"model_data.csv.gz"
infile=gzip.open(input_file,'rb')
incsv=csv.reader(infile)
header_out=incsv.next()

output_file=out_dir+"model_data_ds.csv.gz"
outfile=gzip.open(output_file,'w')
outcsv=csv.writer(outfile)
outcsv.writerow(header_out)

random.seed(1)
for row in incsv:
    if row[-1]=='0': #downsample good
        if random.random()<nontarget_samp_rate:
            outcsv.writerow(row)
    else:
        outcsv.writerow(row)



