import csv
import gzip
import os
import sys
import time
import random
from numpy import *
sys.path.append("/Users/junhe/Documents/workspace/fraud_model/src/csv_operations")
sys.path.append("/Users/junhe/Documents/workspace/fraud_model/prod")

from model_scoring_prod import *


#initialize scoring instance
work_dir = '/Users/junhe/Documents/Data/Model_Data_Signal_Tmx/'
impute_value_filename = work_dir+'impute_values.p'
risk_table_filename = work_dir+'risk_table.p'
var_list_filename= '/Users/junhe/Documents/Results/Model_Results_Signal_Tmx/model_var_list_signal_tmxboth.csv'
model_filename= '/Users/junhe/Documents/Results/Model_Results_Signal_Tmx/model.p'

print "Loading data and model, creating scoring instance"
t0=time.time()
M=model_scoring(impute_value_filename,risk_table_filename,var_list_filename,model_filename)
print "Scoring instance created, timpe lapsed:", time.time()-t0, "sec"

# score test data
work_dir='/Users/junhe/Documents/Data/Model_Data_Signal_Tmx/'
input_file=work_dir+'test_data_dec.csv.gz'
score_output_file='/Users/junhe/Documents/Results/Model_Results_Signal_Tmx/score_dec_full.csv'

infile=gzip.open(input_file,'rb')
incsv=csv.DictReader(infile)
result_key_score=[]

t0=time.time()
nRow=0
for data in incsv:
    #print [data['payment_request_id'],M.score(data)]
    result_key_score.append([data['payment_request_id'],M.score(data)])
    nRow+=1
    if nRow%1000 ==0:
        print nRow,"records scored; time lapsed:",time.time()-t0


# Output test data score results
score_file=open(score_output_file,"w")
score_csv=csv.writer(score_file)
score_csv.writerow(["payment_request_id","score"])
for row in result_key_score:
    score_csv.writerow(row)
score_file.close()

        
        