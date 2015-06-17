import csv
import gzip
import os
import datetime
import sys
from multiprocessing import Pool

# signal name list: id    payment_request_id    signal_id    sgn_bool    sgn_int    sgn_float    sgn_string    time

global work_dir
work_dir="/fraud_model/Data/Raw_Data/signals/"

def roll_up_signal( day_start,  n_Days):
    
    day=day_start #start date
    nDays=n_Days# number of days to process

    prefix='signal_' #prefix to signal numbers
    
    #get signal list
    signalnames_filename=work_dir+"signal_list.csv"
    signalnamesfile=open(signalnames_filename,'rU')
    signalnamescsv=csv.reader(signalnamesfile)
    
    signal_num_list=[]
    for row in signalnamescsv:
        signal_num_list.append(row[0])
    
    signal_num_list=signal_num_list[1:]
    
    signalsnames=[]
    for signame in signal_num_list:
        signalsnames.append(prefix+signame)
    print signalsnames
    
    '''
    # examplar shell script below first
    zcat < fraud_signal_2014-08-01.csv.gz | head -1 > fraud_signal_2014-08-01_sorted.csv 
    zcat < fraud_signal_2014-08-01.csv.gz | sed 1d | sort -t, -k2,2 -k8,8 -k3,3n >> fraud_signal_2014-08-01_sorted.csv 
    gzip fraud_signal_2014-08-01_sorted.csv
    '''
    
    
    for iDay in range(nDays):
        
        print "rolling up signals for day: ",str(day)
        
        cmdout=os.system('zcat < '+work_dir+'fraud_signal_'+str(day)+'.csv.gz | head -1 > '+work_dir+'fraud_signal_'+str(day)+'_sorted.csv')
        cmdout=os.system('zcat < '+work_dir+'fraud_signal_'+str(day)+'.csv.gz | sed 1d | sort -t, -k1,1>> '+work_dir+'fraud_signal_'+str(day)+'_sorted.csv')
        cmdout=os.system('gzip '+work_dir+'fraud_signal_'+str(day)+'_sorted.csv')
        
        
        header_out=['payment_request_id','state','create_time', 'fs_payment_request_id']+signalsnames
        output_file=work_dir+"fraud_signal_flat_"+str(day)+".csv.gz"
        outfile=gzip.open(output_file,'w')
        outcsv=csv.DictWriter(outfile, fieldnames=header_out)
        outcsv.writeheader()
        
        
        input_file=work_dir+"fraud_signal_"+str(day)+"_sorted.csv.gz"
        infile=gzip.open(input_file,'rb')
        incsv=csv.DictReader(infile)
        
        row_flat = {}
        payment_request_id=''
        nRow=0
        nPayment=0
        for row in incsv:
            if row['payment_request_id']=='':
                continue
            # tell if reach new payment
            if row['payment_request_id'] != payment_request_id:
                #output last row if not first time payment_request_id
                if nRow != 0:
                    outcsv.writerow(row_flat)
                    nPayment+=1
                payment_request_id = row['payment_request_id']
                row_flat = {}
                row_flat['payment_request_id'] = row['payment_request_id']
                row_flat['state'] = row['state']
                row_flat['create_time'] = row['create_time']
                row_flat['fs_payment_request_id'] = row['fs_payment_request_id']
                
                
            
            # take signal values
            if row['sgn_bool'] != '':
                signal_val = row['sgn_bool']
            elif row['sgn_int'] != '':
                signal_val = row['sgn_int']
            elif row['sgn_float'] != '':
                signal_val = row['sgn_float']
            elif row['sgn_string'] != '':
                signal_val = row['sgn_string']
            else:
                signal_val = ''
            
            if row['signal_id'] != '' and row['signal_id'] in signal_num_list:
                row_flat[prefix+row['signal_id']] = signal_val
        
            nRow+=1
            if nRow%1000000 ==0:
                print nRow,' rows are processed'
        #end of file output last row_flat
        outcsv.writerow(row_flat)
        nPayment+=1
        print "Totally ", nRow, " rows are processed"
        print "Totally ", nPayment, " payments are processed"
        
        
        #delete intermediate file
        cmdout=os.system('rm '+work_dir+'fraud_signal_'+str(day)+'_sorted.csv.gz')
        
        #increment day by one
        day = day+datetime.timedelta(1)



def roll_up_signal_helper(arg):
    roll_up_signal(arg,1) # always 1 to deal with one day, multiple days are dealt by pool


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

print "last day to roll up signals:",year,'-',month,'-',day
nWorkers = 4
dayEnd = datetime.date(year, month, day)

# prepare datelist to roll up, skip dates that already have been rolled up
dateList = []
for i in range(nDays):
    dayToProcess=dayEnd-datetime.timedelta(i)
    if os.path.exists(work_dir + "fraud_signal_flat_"+str(dayToProcess)+".csv.gz"):
        print "fraud_signal_flat_"+str(dayToProcess)+".csv.gz"," already exits, skipping ..."
    else:
        print "fraud_signal_flat_"+str(dayToProcess)+".csv.gz"," rollup will be processed"
        dateList.append(dayToProcess)


pool = Pool(processes=nWorkers)
pool.map(roll_up_signal_helper, dateList)



