import csv
import gzip
import os
import datetime

from multiprocessing import Pool

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False




def roll_up_tmx_payer( day_start,  n_Days):
    
    day=day_start#start date
    nDays=n_Days # number of days to process
    
    work_dir="/fraud_model/Data/Raw_Data/threatmetrix_payer/"
    prefix='tmx_payer_' #prefix to signal numbers
    
    
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
    
    
    #add reason code indicator signals 
    signal_names=signal_names+reasoncode_signal_names
    #signal_names.remove('tmx_payer_reason_code') # reason code indicator signals are already created
    
    
    for iDay in range(nDays):
        
        print "rolling up threatmetrix signals for day: ",str(day)
        
        cmdout=os.system('zcat < '+work_dir.replace(" ","\ ") +'threatmetrix_payer_'+str(day)+'.csv.gz | head -1 > '+work_dir.replace(" ","\ ")+'threatmetrix_payer_'+str(day)+'_sorted.csv')
        cmdout=os.system('zcat < '+work_dir.replace(" ","\ ")+'threatmetrix_payer_'+str(day)+'.csv.gz | sed 1d | LC_ALL=C sort -t, -k1,1 >> '+work_dir.replace(" ","\ ")+'threatmetrix_payer_'+str(day)+'_sorted.csv')
        cmdout=os.system('gzip '+work_dir.replace(" ","\ ")+'threatmetrix_payer_'+str(day)+'_sorted.csv')
        
        header_out=['payment_request_id','payer_tmx_request_id']+signal_names
        output_file=work_dir+"threatmetrix_payer_flat_"+str(day)+".csv.gz"
        outfile=gzip.open(output_file,'w')
        outcsv=csv.writer(outfile)
        outcsv.writerow(header_out)
        
        
        input_file=work_dir+"threatmetrix_payer_"+str(day)+"_sorted.csv.gz"
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
                    outcsv.writerow([row_flat.get(var,'') for var in header_out])
                    nPayment+=1
                payment_request_id = row['payment_request_id']
                row_flat = {}
                row_flat['payment_request_id'] = row['payment_request_id']
                row_flat['payer_tmx_request_id'] = row['request_id'].replace('-','').lower()
                
            if row['key'] != '':
                # populate signal values in row dict
                signal_val = row['value']
                row_flat[prefix+row['key']] = signal_val
                
                # populate reasoncode indicators
                if row['key']=='reason_code':
                    if row['value'] in reasoncode_names:
                        row_flat[prefix+'rs_ind_'+row['value']]=1
                
        
            nRow+=1
            if nRow%100000 ==0:
                print nRow,' rows are processed'
        #end of file output last row_flat
        outcsv.writerow([row_flat.get(var,'') for var in header_out])
        nPayment+=1
        print "Totally ", nRow, " rows are processed"
        print "Totally ", nPayment, " payments are processed"
        
        
        #delete intermediate file
        cmdout=os.system('rm '+work_dir.replace(" ","\ ")+'threatmetrix_payer_'+str(day)+'_sorted.csv.gz')
        
        #increment day by one
        day = day+datetime.timedelta(1)




def roll_up_tmx_payer_helper(arg):
    roll_up_tmx_payer(arg[0],arg[1])

input_list = (
              [datetime.date(2014,7,1),62],
              [datetime.date(2014,9,1),61],
              [datetime.date(2014,11,1),61],
              )

pool = Pool(processes=3)
pool.map(roll_up_tmx_payer_helper, input_list)
