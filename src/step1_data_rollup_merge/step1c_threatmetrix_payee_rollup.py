import csv
import gzip
import os
import datetime

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False


work_dir="/Users/junhe/Documents/Data/Raw_data/threatmetrix_payee/"
prefix='tmx_payee_' #prefix to signal numbers

day=datetime.date(2014,11,1) #start date
nDays=61 # number of days to process

#get signal list
signalnames_filename=work_dir+"threatmetrix_var_list.csv"
signalnamesfile=open(signalnames_filename,'rU')
signalnamescsv=csv.reader(signalnamesfile)

signal_names=[]
for row in signalnamescsv:
    signal_names.append(row[0])

new_signal_names=[]
for signame in signal_names:
    new_signal_names.append(prefix+signame)
signal_names=new_signal_names


#get reason_code list
reasoncode_names_filename=work_dir+"reasoncode_list.csv"
reasoncodenamesfile=open(reasoncode_names_filename,'rU')
reasoncodenamescsv=csv.reader(reasoncodenamesfile)

reasoncode_names=[]
reasoncode_signal_names=[]
for row in reasoncodenamescsv:
    reasoncode_names.append(row[0])
    reasoncode_signal_names.append(prefix+'rs_ind_'+row[0])


#get tmx_reason_code list
tmx_reasoncode_names_filename=work_dir+"tmx_reasoncode_list.csv"
tmx_reasoncodenamesfile=open(tmx_reasoncode_names_filename,'rU')
tmx_reasoncodenamescsv=csv.reader(tmx_reasoncodenamesfile)

tmx_reasoncode_names=[]
tmx_reasoncode_signal_names=[]
for row in tmx_reasoncodenamescsv:
    tmx_reasoncode_names.append(row[0])
    tmx_reasoncode_signal_names.append(prefix+'tmxrs_ind_'+row[0])
    

#add reason code indicator signals 
signal_names=signal_names+reasoncode_signal_names+tmx_reasoncode_signal_names
print signal_names
#signal_names.remove('tmx_payee_reason_code') # reason code indicator signals are already created


for iDay in range(nDays):
    
    print "rolling up threatmetrix signals for day: ",str(day)
    
    
    cmdout=os.system('zcat < '+work_dir+'threatmetrix_payee_'+str(day)+'.csv.gz | head -1 > '+work_dir+'threatmetrix_payee_'+str(day)+'_sorted.csv')
    cmdout=os.system('zcat < '+work_dir+'threatmetrix_payee_'+str(day)+'.csv.gz | sed 1d | LC_ALL=C sort -t, -k1,1 >> '+work_dir+'threatmetrix_payee_'+str(day)+'_sorted.csv')
    cmdout=os.system('gzip '+work_dir+'threatmetrix_payee_'+str(day)+'_sorted.csv')
    
    
    header_out=['payment_request_id']+signal_names
    output_file=work_dir+"threatmetrix_payee_flat_"+str(day)+".csv.gz"
    outfile=gzip.open(output_file,'w')
    outcsv=csv.DictWriter(outfile, fieldnames=header_out)
    outcsv.writeheader()
    
    
    input_file=work_dir+"threatmetrix_payee_"+str(day)+"_sorted.csv.gz"
    infile=gzip.open(input_file,'rb')
    incsv=csv.DictReader(infile)
    
    row_flat = {}
    payment_request_id=''
    nRow=0
    nPayment=0
    for row in incsv:
        if not is_number(row['payment_request_id']) or row['payment_request_id'] == '':
            print "key not valid: ",row['payment_request_id']
            continue
        # tell if reach new payment
        if row['payment_request_id'] != payment_request_id:
            #output last row if not first time payment_request_id
            if nRow != 0:
                #print row_flat
                outcsv.writerow(row_flat)
                nPayment+=1
            payment_request_id = row['payment_request_id']
            row_flat = {}
            row_flat['payment_request_id'] = row['payment_request_id']
            
        if row['key'] != '':
            # populate signal values in row dict
            signal_val = row['value']
            row_flat[prefix+row['key']] = signal_val
            
            # populate reasoncode indicators
            if row['key']=='reason_code':
                if row['value'] in reasoncode_names:
                    row_flat[prefix+'rs_ind_'+row['value']]=1
            
            # populate reasoncode indicators
            if row['key']=='tmx_reason_code':
                #print row['value']
                if row['value'] in tmx_reasoncode_names:
                    row_flat[prefix+'tmxrs_ind_'+row['value']]=1
    
        nRow+=1
        if nRow%100000 ==0:
            print nRow,' rows are processed'
    #end of file output last row_flat
    outcsv.writerow(row_flat)
    nPayment+=1
    print "Totally ", nRow, " rows are processed"
    print "Totally ", nPayment, " payments are processed"
    
    
    #delete intermediate file
    cmdout=os.system('rm '+work_dir+'threatmetrix_payee_'+str(day)+'_sorted.csv.gz')
    
    #increment day by one
    day = day+datetime.timedelta(1)



