import csv
import gzip
import os
import datetime
import random
import sys
sys.path.append("/fraud_model/Code/tools/csv_operations")
import csv_ops
from csv_ops import *



# load tmx_reason_code to dict
tmxrc_filename = '/fraud_model/Data/Raw_Data/tmx_reason_code/tmx_reason_code_jul_dec.csv.gz'
tmxrc_file = gzip.open(tmxrc_filename,'rb')
tmxrc_csv = csv.reader(tmxrc_file)

tmxrc_data = {}
nRow=0
for row in tmxrc_csv:
    tmxrc_data[row[0]] = row[1]
    nRow+=1

print "tmx_reason_code data loaded, nRows=",len(tmxrc_data)


'''
# patch rtmx_reason_code for payer
day=datetime.date(2015,1,1)
nDays=59

for iDay in range(nDays):
    
    input_filename="/fraud_model/Data/Raw_Data/threatmetrix_payer/threatmetrix_payer_"+str(day)+".csv.gz"
    output_filename="/fraud_model/Data/Raw_Data/threatmetrix_payer_w_tmxrc/threatmetrix_payer_"+str(day)+".csv.gz"
    
    header_out=['payment_request_id','threatmetrix_request_time','request_id','request_type','threatmetrix_request_id','key','value']
    output_file = gzip.open(output_filename,'w')
    outcsv = csv.writer(output_file)
    
    # write original file to disk first
    input_file = gzip.open(input_filename,'rb')
    incsv = csv.DictReader(input_file)
    outcsv.writerow(header_out)
    for row in incsv:
        outcsv.writerow([row[var] for var in header_out])
    print "Write original file done, processing additional tmx_reason_code"
    
    
    # patch downloaded tmx after original file 
    input_file = gzip.open(input_filename,'rb') #open inpurt file again to reset the pointer
    incsv = csv.DictReader(input_file)
    
    tmx_data_dedupe=set([]) # pool to track unique request_id
    for row in incsv:
        if row['request_id'] not in tmx_data_dedupe:
            tmx_data_dedupe.add(row['request_id'])
            tmp_request_id=row['request_id'].replace('-','').lower()
            if  tmp_request_id in tmxrc_data:
                tmx_reason_code_list=tmxrc_data[tmp_request_id].replace('{','').replace('}','').split(',')
                for tmx_reason_code in tmx_reason_code_list:
                    new_row=copy.copy(row)
                    new_row['key']='tmx_reason_code'
                    new_row['value']=tmx_reason_code
                    outcsv.writerow([new_row[var] for var in header_out])
    
    day = day+datetime.timedelta(1)
    
    
'''
# patch rtmx_reason_code for payee
day=datetime.date(2015,1,1)
nDays=59

for iDay in range(nDays):
    
    input_filename="/fraud_model/Data/Raw_Data/threatmetrix_payee/threatmetrix_payee_"+str(day)+".csv.gz"
    output_filename="/fraud_model/Data/Raw_Data/threatmetrix_payee_w_tmxrc/threatmetrix_payee_"+str(day)+".csv.gz"
    
    header_out=['payment_request_id','threatmetrix_request_time','request_id','request_type','threatmetrix_request_id','key','value']
    output_file = gzip.open(output_filename,'w')
    outcsv = csv.writer(output_file)
    
    # write original file to disk first
    input_file = gzip.open(input_filename,'rb')
    incsv = csv.DictReader(input_file)
    outcsv.writerow(header_out)
    for row in incsv:
        outcsv.writerow([row[var] for var in header_out])
    print "Write original file done, processing additional tmx_reason_code"
    
    
    # patch downloaded tmx after original file 
    input_file = gzip.open(input_filename,'rb') #open inpurt file again to reset the pointer
    incsv = csv.DictReader(input_file)
    
    tmx_data_dedupe=set([]) # pool to track unique request_id
    for row in incsv:
        if row['request_id'] not in tmx_data_dedupe:
            tmx_data_dedupe.add(row['request_id'])
            tmp_request_id=row['request_id'].replace('-','').lower()
            if  tmp_request_id in tmxrc_data:
                tmx_reason_code_list=tmxrc_data[tmp_request_id].replace('{','').replace('}','').split(',')
                for tmx_reason_code in tmx_reason_code_list:
                    new_row=copy.copy(row)
                    new_row['key']='tmx_reason_code'
                    new_row['value']=tmx_reason_code
                    outcsv.writerow([new_row[var] for var in header_out])
    
    day = day+datetime.timedelta(1)
    




