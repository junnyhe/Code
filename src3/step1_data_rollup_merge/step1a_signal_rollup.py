import csv
import gzip
import os
import datetime


# signal name list: id    payment_request_id    signal_id    sgn_bool    sgn_int    sgn_float    sgn_string    time



work_dir="/fraud_model/Data/Raw_Data/signals/"
prefix='signal_' #prefix to signal numbers

day=datetime.date(2015,2,1) #start date
nDays=28 # number of days to process

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
        if nRow%100000 ==0:
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



