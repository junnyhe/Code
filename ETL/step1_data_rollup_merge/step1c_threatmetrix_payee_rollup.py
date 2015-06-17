import csv
import gzip
import os
import datetime
import sys

from multiprocessing import Pool

global work_dir
work_dir="/fraud_model/Data/Raw_Data/threatmetrix_payee_w_tmxrc/"

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False


def roll_up_tmx_payee( day_start,  n_Days):
    
    day=day_start#start date
    nDays=n_Days # number of days to process
    
    prefix='tmx_payee_' #prefix to signal numbers

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
    
    print signal_names
    
    
    for iDay in range(nDays):
        
        print "rolling up payee threatmetrix signals for day: ",str(day)
        
        
        cmdout=os.system('zcat < '+work_dir.replace(" ","\ ")+'threatmetrix_payee_'+str(day)+'.csv.gz | head -1 > '+work_dir.replace(" ","\ ")+'threatmetrix_payee_'+str(day)+'_sorted.csv')
        cmdout=os.system('zcat < '+work_dir.replace(" ","\ ")+'threatmetrix_payee_'+str(day)+'.csv.gz | sed 1d | LC_ALL=C sort -t, -k1,1 >> '+work_dir.replace(" ","\ ")+'threatmetrix_payee_'+str(day)+'_sorted.csv')
        cmdout=os.system('gzip '+work_dir.replace(" ","\ ")+'threatmetrix_payee_'+str(day)+'_sorted.csv')
        
        
        header_out=['payment_request_id','payee_tmx_request_id']+signal_names
        output_file=work_dir+"threatmetrix_payee_flat_"+str(day)+".csv.gz"
        outfile=gzip.open(output_file,'w')
        outcsv=csv.writer(outfile)
        outcsv.writerow(header_out)
        
        
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
                    outcsv.writerow([row_flat.get(var,'') for var in header_out])
                    nPayment+=1
                payment_request_id = row['payment_request_id']
                row_flat = {}
                row_flat['payment_request_id'] = row['payment_request_id']
                row_flat['payee_tmx_request_id'] = row['request_id'].replace('-','').lower()
                row_flat[prefix+'reason_code'] = []
                row_flat[prefix+'tmx_reason_code'] = []
                
            if row['key'] != '':
                # populate reason_code list
                if row['key']=='reason_code':
                    row_flat[ prefix+'reason_code'].append(row['value'])
                # populate tmx_reason_code list
                elif row['key']=='tmx_reason_code':
                    row_flat[ prefix+'tmx_reason_code'].append(row['value'])
                # populate other signal values in row dict
                else:
                    signal_val = row['value']
                    row_flat[prefix+row['key']] = signal_val
                        
        
            nRow+=1
            if nRow%500000 ==0:
                print nRow,' rows are processed for:', str(day)
        #end of file output last row_flat
        outcsv.writerow([row_flat.get(var,'') for var in header_out])
        nPayment+=1
        print "Totally ", nRow, " rows are processed"
        print "Totally ", nPayment, " payments are processed"
        
        
        #delete intermediate file
        cmdout=os.system('rm '+work_dir.replace(" ","\ ")+'threatmetrix_payee_'+str(day)+'_sorted.csv.gz')
        
        #increment day by one
        day = day+datetime.timedelta(1)
    


def roll_up_tmx_payee_helper(arg):
    roll_up_tmx_payee(arg,1) # always 1 to deal with one day, multiple days are dealt by pool

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

print "first day to roll up tmx payee:",year,'-',month,'-',day
nWorkers = 4
dayEnd = datetime.date(year, month, day)

# prepare datelist to roll up, skip dates that already have been rolled up
dateList = []
for i in range(nDays):
    dayToProcess=dayEnd-datetime.timedelta(i)
    if os.path.exists(work_dir + "threatmetrix_payee_flat_"+str(dayToProcess)+".csv.gz"):
        print "threatmetrix_payee_flat_"+str(dayToProcess)+".csv.gz"," already exits, skipping ..."
    else:
        print "threatmetrix_payee_flat_"+str(dayToProcess)+".csv.gz"," rollup will be processed"
        dateList.append(dayToProcess)


pool = Pool(processes=nWorkers)
pool.map(roll_up_tmx_payee_helper, dateList)


