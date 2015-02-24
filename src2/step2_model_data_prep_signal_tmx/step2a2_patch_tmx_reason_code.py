import csv
import gzip
import os
import datetime
import random
import sys
sys.path.append("/home/junhe/fraud_model/Code/tools/csv_operations")
import csv_ops
from csv_ops import *



#input_file = '/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt/model_data_pmt.csv.gz'
#output_file = '/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt/model_data_pmt_sorted_payee.csv.gz'
#cmdout=os.system( 'zcat < '+input_file.replace(" ","\ ") + '| LC_ALL=C sort -t, -516,516 | gzip >> '+output_file.replace(" ","\ ") )


global tmxrc_data

# load tmx_reason code
tmxrc_filename = '/home/junhe/fraud_model/Data/Raw_Data/tmx_reason_code/tmx_reason_code_jul_dec.csv.gz'
tmxrc_file = gzip.open(tmxrc_filename,'rb')
tmxrc_csv = csv.reader(tmxrc_file)

tmxrc_data = {}
for row in tmxrc_csv:
    tmxrc_data[row[0]] = row[1]

print "tmx_reason_code data loaded, nRows=",len(tmxrc_data)
    
    
def patch_tmx_reason_code(input_filename,output_filename):
    
    print "processing tmx patching for file:", input_filename
    
    # join tmx_reason_code with input data
    
    input_file = gzip.open(input_filename,'rb')
    incsv = csv.DictReader(input_file)
    header=incsv.fieldnames
    
    
    output_file = gzip.open(output_filename ,'w')
    outcsv = csv.writer(output_file)
    outcsv.writerow(header)
    
    nRow=0
    for row in incsv:
        
        if row['payer_tmx_request_id'] in tmxrc_data:
            row['tmx_payer_tmx_reason_code'] = tmxrc_data[row['payer_tmx_request_id'] ]
        else:
            row['tmx_payer_tmx_reason_code']=''
            
        if row['payee_tmx_request_id'] in tmxrc_data:
            row['tmx_payee_tmx_reason_code'] = tmxrc_data[row['payee_tmx_request_id'] ]
        else: row['tmx_payee_tmx_reason_code']=''
        
        outcsv.writerow([row[var] for var in header])
        
        nRow+=1
        if nRow%10000 ==0:
            print nRow,' rows are processed'


input_filename = '/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt/model_data_pmt.csv.gz'
output_filename = '/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt/model_data_pmt_tmxpatched.csv.gz'
patch_tmx_reason_code(input_filename,output_filename)

input_filename = '/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt/model_data_wd.csv.gz'
output_filename = '/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt/model_data_wd_tmxpatched.csv.gz'
patch_tmx_reason_code(input_filename,output_filename)


input_filename = '/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt/test_data_sept_pmt.csv.gz'
output_filename = '/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt/test_data_sept_pmt_tmxpatched.csv.gz'
patch_tmx_reason_code(input_filename,output_filename)

input_filename = '/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt/test_data_sept_wd.csv.gz'
output_filename = '/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt/test_data_sept_wd_tmxpatched.csv.gz'
patch_tmx_reason_code(input_filename,output_filename)


input_filename = '/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt/test_data_oct_pmt.csv.gz'
output_filename = '/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt/test_data_oct_pmt_tmxpatched.csv.gz'
patch_tmx_reason_code(input_filename,output_filename)

input_filename = '/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt/test_data_oct_wd.csv.gz'
output_filename = '/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt/test_data_oct_wd_tmxpatched.csv.gz'
patch_tmx_reason_code(input_filename,output_filename)


input_filename = '/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt/test_data_nov_pmt.csv.gz'
output_filename = '/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt/test_data_nov_pmt_tmxpatched.csv.gz'
patch_tmx_reason_code(input_filename,output_filename)

input_filename = '/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt/test_data_nov_wd.csv.gz'
output_filename = '/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt/test_data_nov_wd_tmxpatched.csv.gz'
patch_tmx_reason_code(input_filename,output_filename)


input_filename = '/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt/test_data_dec_pmt.csv.gz'
output_filename = '/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt/test_data_dec_pmt_tmxpatched.csv.gz'
patch_tmx_reason_code(input_filename,output_filename)

input_filename = '/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt/test_data_dec_wd.csv.gz'
output_filename = '/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt/test_data_dec_wd_tmxpatched.csv.gz'
patch_tmx_reason_code(input_filename,output_filename)



