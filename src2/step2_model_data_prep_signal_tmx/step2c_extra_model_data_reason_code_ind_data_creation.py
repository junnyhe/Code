import csv
import gzip
import os
import sys
import time
import datetime
import random
from numpy import *
from operator import itemgetter
sys.path.append("/home/junhe/fraud_model/Code/tools/csv_operations")
import csv_ops
from csv_ops import *
from multiprocessing import Pool

import nltk
import string
import os

from sklearn.feature_extraction.text import TfidfVectorizer
from nltk.stem.porter import PorterStemmer

    
def compute_rc_stats(input_file,output_file, rc_ind_file,rc_var_name):
    
    #input_file="/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt/model_data_pmt_tmxpatched.csv.gz"
    #output_file="/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt/"+out_stats_filename
    #rc_ind_file="/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt/"+rc_ind_var_list_file
    
    infile=gzip.open(input_file,'rb')
    incsv=csv.DictReader(infile)

    outfile=open(output_file,'w')
    outcsv=csv.writer(outfile)
    
    rcindfile=open(rc_ind_file,'w')
    rcindcsv=csv.writer(rcindfile)
    
    
    rc_dict={}
    nRow=0
    for row in incsv:
        rc_list_in_row = row[rc_var_name].replace('{','').replace('}','').split(',')
        for rc in rc_list_in_row:
            if rc not in rc_dict:
                rc_dict[rc]={}
                rc_dict[rc]['n']=1
                rc_dict[rc]['n_tgt']=0
            else:
                rc_dict[rc]['n']+=1
            if row['target']=='1':
                rc_dict[rc]['n_tgt']=rc_dict[rc].get('n_tgt',0)+1
        
        nRow+=1
        #if nRow>500000:
        #    break
    
    for key in rc_dict:
        rc_dict[key]['rate']=rc_dict[key]['n_tgt']/float(rc_dict[key]['n'])
    
    
    # save risk rating of reason code to disk
    #rc_rank = [[key,rc_dict[key]['n'],rc_dict[key]['n_tgt'],rc_dict[key]['rate']] for key in rc_dict if rc_dict[key]['n_tgt']>=1]
    rc_rank = [[key,rc_dict[key]['n'],rc_dict[key]['n_tgt'],rc_dict[key]['rate']] for key in rc_dict]
    rc_rank = sorted(rc_rank, key=itemgetter(3))
    outcsv.writerow(['tmx_rc','total','n_tgt','tgt_rate'])
    for row in rc_rank:
        outcsv.writerow(row)
    
    
    # create reason code list to create indicator
    rc_var_list = [key for key in rc_dict if rc_dict[key]['n_tgt']>=1]
    for var in rc_var_list:
        rcindcsv.writerow([var])
    
    
    print len(rc_var_list)
    print len(rc_rank)
    print nRow
    
    infile.close()
    rcindfile.close()
    outfile.close()



'''
input_file="/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt/model_data_pmt_ins_ds.csv.gz"
output_file="/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt/model_data_pmt_ins_ds_tmxrc_risk_rate_payer.csv"
rc_ind_file="/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt/var_list_rc_ind_payer.csv"
compute_rc_stats(input_file, output_file, rc_ind_file, rc_var_name='tmx_payer_tmx_reason_code')


input_file="/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt/model_data_pmt_ins_ds.csv.gz"
output_file="/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt/model_data_pmt_ins_ds_tmxrc_risk_rate_payee.csv"
rc_ind_file="/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt/var_list_rc_ind_payee.csv"
compute_rc_stats(input_file, output_file, rc_ind_file, rc_var_name='tmx_payee_tmx_reason_code')
'''

def rc_ind_creation_one_row(row,header,payer_rc_var_set,payee_rc_var_set,str_detect_list ):
    
    
    # initialize with zeros
    row_rc_ind={}
    for var in header:
        row_rc_ind[var]=0 
    
    # populate payer if in risk list
    if row['tmx_payer_tmx_reason_code'] != '':
        payer_rc_list_in_row = row['tmx_payer_tmx_reason_code'].replace('{','').replace('}','').split(',')
        for rc in payer_rc_list_in_row:
            if rc in payer_rc_var_set:
                row_rc_ind["payer_rc_ind"+rc]=1
            for str in str_detect_list:
                if str in rc:
                    row_rc_ind["payer_rc_ind_"+str]=1


    
    # populate payee if in risk list
    if row['tmx_payee_tmx_reason_code'] != '':
        payee_rc_list_in_row = row['tmx_payee_tmx_reason_code'].replace('{','').replace('}','').split(',')
        for rc in payee_rc_list_in_row:
            if rc in payee_rc_var_set:
                row_rc_ind["payee_rc_ind"+rc]=1
            for str in str_detect_list:
                if str in rc:
                    row_rc_ind["payee_rc_ind_"+str]=1
    
    # set target
    row_rc_ind['payment_request_id'] = row['payment_request_id']
    row_rc_ind['target'] = row['target']
    row_rc_ind['manual_review'] = row['manual_review']
    'manual_review'
    
    return row_rc_ind


def rc_ind_creation(input_file,output_file):
    
    str_detect_list = ["_T_TOR","VPN","_PossibleVM","_Malware"]
    
    infile=gzip.open(input_file,'rb')
    incsv=csv.DictReader(infile)

    
    outfile=gzip.open(output_file,'w')
    outcsv=csv.writer(outfile)
    
    # payer rc var list 
    payer_rc_var_filename='/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt/var_list_rc_ind_payer.csv'
    payer_rc_var_file=open(payer_rc_var_filename,'rU')
    payer_rc_var_csv=csv.reader(payer_rc_var_file)
    payer_rc_var_list=[]
    for row in payer_rc_var_csv:
        payer_rc_var_list.append(row[0])
    payer_rc_var_set=set(payer_rc_var_list)
    
    # payee rc var list 
    payee_rc_var_filename='/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt/var_list_rc_ind_payee.csv'
    payee_rc_var_file=open(payee_rc_var_filename,'rU')
    payee_rc_var_csv=csv.reader(payee_rc_var_file)
    payee_rc_var_list=[]
    for row in payee_rc_var_csv:
        payee_rc_var_list.append(row[0])
    payee_rc_var_set=set(payee_rc_var_list)
    
    header=["payment_request_id"] +\
    ["payer_rc_ind"+var for var in payer_rc_var_list] + \
    ["payee_rc_ind"+var for var in payee_rc_var_list] + \
    ["payer_rc_ind_"+str for str in str_detect_list] +\
    ["payee_rc_ind_"+str for str in str_detect_list] +\
    ['target','manual_review']
    outcsv.writerow(header)
    
    nRow=0
    for row in incsv:
        
        row_rc_ind=rc_ind_creation_one_row(row,header,payer_rc_var_set,payee_rc_var_set,str_detect_list)
        
        outcsv.writerow([row_rc_ind[key] for key in header])
        
        nRow+=1
        if nRow%10000 ==0:
            print nRow," rows are processed for file:",input_file


def rc_ind_creation_helper(arg):
    rc_ind_creation(arg[0],arg[1])
    

work_dir = "/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt/"

input_list = (
              [work_dir+"model_data_pmt_ins_ds.csv.gz",work_dir+"model_data_pmt_ins_ds_rc_ind.csv.gz"],
              [work_dir+"model_data_pmt_oos_ds.csv.gz",work_dir+"model_data_pmt_oos_ds_rc_ind.csv.gz"],
              [work_dir+"test_data_sept_pmt_ds.csv.gz",work_dir+"test_data_sept_pmt_ds_rc_ind.csv.gz"],
              [work_dir+"test_data_oct_pmt_ds.csv.gz",work_dir+"test_data_oct_pmt_ds_rc_ind.csv.gz"],
              [work_dir+"test_data_nov_pmt_ds.csv.gz",work_dir+"test_data_nov_pmt_ds_rc_ind.csv.gz"],
              [work_dir+"test_data_dec_pmt_ds.csv.gz",work_dir+"test_data_dec_pmt_ds_rc_ind.csv.gz"],
              )
            # Inputs: rc_ind_creation(input_file,output_file)
pool = Pool(processes=3)
pool.map(rc_ind_creation_helper, input_list)


'''
#===============================================================================
# Old non-parallel
#===============================================================================
input_file="/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt/model_data_pmt_ins_ds.csv.gz"
output_file="/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt/model_data_pmt_ins_ds_rc_ind.csv.gz"
rc_ind_creation(input_file,output_file)

input_file="/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt/model_data_pmt_oos_ds.csv.gz"
output_file="/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt/model_data_pmt_oos_ds_rc_ind.csv.gz"
rc_ind_creation(input_file,output_file)

input_file="/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt/test_data_sept_pmt_ds.csv.gz"
output_file="/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt/test_data_sept_pmt_ds_rc_ind.csv.gz"
rc_ind_creation(input_file,output_file)

input_file="/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt/test_data_oct_pmt_ds.csv.gz"
output_file="/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt/test_data_oct_pmt_ds_rc_ind.csv.gz"
rc_ind_creation(input_file,output_file)

input_file="/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt/test_data_nov_pmt_ds.csv.gz"
output_file="/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt/test_data_nov_pmt_ds_rc_ind.csv.gz"
rc_ind_creation(input_file,output_file)

input_file="/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt/test_data_dec_pmt_ds.csv.gz"
output_file="/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt/test_data_dec_pmt_ds_rc_ind.csv.gz"
rc_ind_creation(input_file,output_file)

'''
















