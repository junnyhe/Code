import csv
import gzip
import os
import sys
import time
import datetime
import random
from numpy import *
from operator import itemgetter
sys.path.append("/fraud_model/Code/tools/csv_operations")
import csv_ops
from csv_ops import *
from multiprocessing import Pool


from sklearn.feature_extraction.text import TfidfVectorizer

    
def compute_tmxrc_stats(input_file,output_file, rc_ind_file,rc_var_name):
    
    #input_file="/fraud_model/Data/Model_Data_Signal_Tmx_v3pmt/model_data_pmt_tmxpatched.csv.gz"
    #output_file="/fraud_model/Data/Model_Data_Signal_Tmx_v3pmt/"+out_stats_filename
    #rc_ind_file="/fraud_model/Data/Model_Data_Signal_Tmx_v3pmt/"+rc_ind_var_list_file
    
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
input_file="/fraud_model/Data/Model_Data_Signal_Tmx_v3pmt/model_data_pmt_ins_ds.csv.gz"
output_file="/fraud_model/Data/Model_Data_Signal_Tmx_v3pmt/model_data_pmt_ins_ds_tmxrc_risk_rate_payer.csv"
rc_ind_file="/fraud_model/Data/Model_Data_Signal_Tmx_v3pmt/entry_list_tmxrc_payer.csv"
compute_tmxrc_stats(input_file, output_file, rc_ind_file, rc_var_name='tmx_payer_tmx_reason_code')


input_file="/fraud_model/Data/Model_Data_Signal_Tmx_v3pmt/model_data_pmt_ins_ds.csv.gz"
output_file="/fraud_model/Data/Model_Data_Signal_Tmx_v3pmt/model_data_pmt_ins_ds_tmxrc_risk_rate_payee.csv"
rc_ind_file="/fraud_model/Data/Model_Data_Signal_Tmx_v3pmt/entry_list_tmxrc_payee.csv"
compute_tmxrc_stats(input_file, output_file, rc_ind_file, rc_var_name='tmx_payee_tmx_reason_code')
'''

def tmxrc_ind_creation_one_row(row,header,payer_tmxrc_var_set,payee_tmxrc_var_set,str_detect_list ):
    
    # initialize with zeros
    row_tmxrc_ind={}
    for var in header:
        row_tmxrc_ind[var]=0 
    
    # populate payer if in risk list
    if row['tmx_payer_tmx_reason_code'] != '':
        exec "payer_tmxrc_list_in_row ="+row['tmx_payer_tmx_reason_code'].replace(';',',')
        for rc in payer_tmxrc_list_in_row:
            if rc in payer_tmxrc_var_set:
                row_tmxrc_ind["payer_tmxrc_ind"+rc]=1
            for str in str_detect_list:
                if str in rc:
                    row_tmxrc_ind["payer_tmxrc_ind_"+str]=1


    
    # populate payee if in risk list
    if row['tmx_payee_tmx_reason_code'] != '':
        exec "payee_tmxrc_list_in_row ="+ row['tmx_payee_tmx_reason_code'].replace(';',',')
        for rc in payee_tmxrc_list_in_row:
            if rc in payee_tmxrc_var_set:
                row_tmxrc_ind["payee_tmxrc_ind"+rc]=1
            for str in str_detect_list:
                if str in rc:
                    row_tmxrc_ind["payee_tmxrc_ind_"+str]=1
    
    # set target
    row_tmxrc_ind['payment_request_id'] = row['payment_request_id']
    row_tmxrc_ind['target'] = row['target']
    row_tmxrc_ind['manual_review'] = row['manual_review']
    'manual_review'
    
    return row_tmxrc_ind


def tmxrc_ind_creation(input_file,output_file):
    
    str_detect_list = ["_T_TOR","VPN","_PossibleVM","_Malware"]
    
    infile=gzip.open(input_file,'rb')
    incsv=csv.DictReader(infile)

    
    outfile=gzip.open(output_file,'w')
    outcsv=csv.writer(outfile)
    
    # payer rc var list 
    payer_tmxrc_var_filename='/fraud_model/Data/Model_Data_Signal_Tmx_v3pmt/entry_list_tmxrc_payer.csv'
    payer_tmxrc_var_file=open(payer_tmxrc_var_filename,'rU')
    payer_tmxrc_var_csv=csv.reader(payer_tmxrc_var_file)
    payer_tmxrc_var_list=[]
    for row in payer_tmxrc_var_csv:
        payer_tmxrc_var_list.append(row[0])
    payer_tmxrc_var_set=set(payer_tmxrc_var_list)
    
    
    # payee rc var list 
    payee_tmxrc_var_filename='/fraud_model/Data/Model_Data_Signal_Tmx_v3pmt/entry_list_tmxrc_payee.csv'
    payee_tmxrc_var_file=open(payee_tmxrc_var_filename,'rU')
    payee_tmxrc_var_csv=csv.reader(payee_tmxrc_var_file)
    payee_tmxrc_var_list=[]
    for row in payee_tmxrc_var_csv:
        payee_tmxrc_var_list.append(row[0])
    payee_tmxrc_var_set=set(payee_tmxrc_var_list)
    
    
    header=["payment_request_id"] +\
    ["payer_tmxrc_ind"+var for var in payer_tmxrc_var_list] + \
    ["payee_tmxrc_ind"+var for var in payee_tmxrc_var_list] + \
    ["payer_tmxrc_ind_"+str for str in str_detect_list] +\
    ["payee_tmxrc_ind_"+str for str in str_detect_list] +\
    ['target','manual_review']
    outcsv.writerow(header)
    
    
    nRow=0
    for row in incsv:
        
        row_tmxrc_ind=tmxrc_ind_creation_one_row(row,header,payer_tmxrc_var_set,payee_tmxrc_var_set,str_detect_list)
        
        outcsv.writerow([row_tmxrc_ind[key] for key in header])
        
        nRow+=1
        if nRow%10000 ==0:
            print nRow," rows are processed for file:",input_file


def tmxrc_ind_creation_helper(arg):
    tmxrc_ind_creation(arg[0],arg[1])
    

work_dir = "/fraud_model/Data/Model_Data_Signal_Tmx_v3pmt/"

input_list = (
              [work_dir+"model_data_pmt_ins_ds.csv.gz",work_dir+"model_data_pmt_ins_ds_tmxrc_ind.csv.gz"],
              [work_dir+"model_data_pmt_oos_ds.csv.gz",work_dir+"model_data_pmt_oos_ds_tmxrc_ind.csv.gz"],
              [work_dir+"test_data_sept_pmt_ds.csv.gz",work_dir+"test_data_sept_pmt_ds_tmxrc_ind.csv.gz"],
              [work_dir+"test_data_oct_pmt_ds.csv.gz",work_dir+"test_data_oct_pmt_ds_tmxrc_ind.csv.gz"],
              [work_dir+"test_data_nov_pmt_ds.csv.gz",work_dir+"test_data_nov_pmt_ds_tmxrc_ind.csv.gz"],
              [work_dir+"test_data_dec_pmt_ds.csv.gz",work_dir+"test_data_dec_pmt_ds_tmxrc_ind.csv.gz"],
              )
            # Inputs: tmxrc_ind_creation(input_file,output_file)
pool = Pool(processes=3)
pool.map(tmxrc_ind_creation_helper, input_list)

