import csv
import gzip
import os
import sys
import time
import datetime
import random
from numpy import *
sys.path.append("/fraud_model/Code/tools/csv_operations")
import csv_ops
from csv_ops import *

from math import radians, cos, sin, asin, sqrt
from multiprocessing import Pool



def geodist(lat1, lon1, lat2,  lon2):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 

    # 6367 km is the radius of the Earth
    dist = 6367 * c /1.8 # distance in miles
    return dist

def levenshtein(s1, s2):
    """
    calculate string distance given by: 
    http:/en.wikibooks.org/wiki/Algorithm_Implementation/Strings/Levenshtein_distance
    """
    
    if len(s1) < len(s2):
        return levenshtein(s2, s1)
 
    # len(s1) >= len(s2)
    if len(s2) == 0:
        return len(s1)
 
    previous_row = range(len(s2) + 1)
    for i, c1 in enumerate(s1):
        current_row = [i + 1]
        for j, c2 in enumerate(s2):
            insertions = previous_row[j + 1] + 1 # j+1 instead of j since previous_row and current_row are one character longer
            deletions = current_row[j] + 1       # than s2
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row
 
    return previous_row[-1]


################################################################################
# feature engineering step 1                                                   #
################################################################################

def time_str_covert(input_time):
    if input_time != '':
        ymd_list=input_time.split('-')
        year=int(ymd_list[0])
        month=int(ymd_list[1])
        day=int(ymd_list[2])
        return datetime.date(year, month, day)
    else:
        return None

def feature_creation_one_row(row,time_var_list,ip_var_list,ppcmp_var_list,leven_dist_var_list,zip_var_list):
        
    # datetime difference vars
    create_time = datetime.date.fromtimestamp( int(row['create_time']) )
    for var in time_var_list:
        try:
            days_diff = create_time - time_str_covert(row[var])
            row['df_'+var] = days_diff.days
            row['md_df_'+var] = 0 #missing ind
        except:
            row['df_'+var] = ''
            row['md_df_'+var] = 1 #missing ind
    
    try:
        days_diff = create_time - datetime.date.fromtimestamp( float(row['signal_159']) )
        row['df_signal159_payee_last_review']=days_diff.days
        row['md_df_signal159_payee_last_review']=0 #missing ind
    except:
        row['df_signal159_payee_last_review']=""
        row['md_df_signal159_payee_last_review']=1 #missing ind
        
    try:
        days_diff = create_time - datetime.date.fromtimestamp( float(row['signal_169']) )
        row['df_signal169_payer_last_review']=days_diff.days
        row['md_df_signal169_payer_last_review']=0 #missing ind
    except:
        row['df_signal169_payer_last_review']=""
        row['md_df_signal169_payer_last_review']=1 #missing ind
        


   
    row['payee_reviewed_le_7d']=0
    row['payee_reviewed_7_30d']=0
    row['payee_reviewed_30_60d']=0
    if row['df_signal159_payee_last_review'] =='':
        row['payee_reviewed_ever']=0
    else:
        row['payee_reviewed_ever']=1
        if float(row['df_signal159_payee_last_review'])<=7:
            row['payee_reviewed_le_7d']=1
        elif float(row['df_signal159_payee_last_review'])<=30:
            row['payee_reviewed_7_30d']=1
        elif float(row['df_signal159_payee_last_review']) <=60:
            row['payee_reviewed_30_60d']=1

    
    row['payer_reviewed_le_7d']=0
    row['payer_reviewed_7_30d']=0
    row['payer_reviewed_30_60d']=0
    if row['df_signal169_payer_last_review'] =='':
        row['payer_reviewed_ever']=0
    else:
        row['payer_reviewed_ever']=1
        if float(row['df_signal169_payer_last_review'])<=7:
            row['payer_reviewed_le_7d']=1
        elif float(row['df_signal169_payer_last_review'])<=30:
            row['payer_reviewed_7_30d']=1
        elif float(row['df_signal169_payer_last_review']) <=60:
            row['payer_reviewed_30_60d']=1
        
    # ip2,3 vars
    for var in ip_var_list:
        try:
            ip_dig_list=row[var].split('.')
            row['ip2_'+var] = '.'.join(ip_dig_list[0:2])
            row['ip3_'+var] = '.'.join(ip_dig_list[0:3])
        except:
            row['ip2_'+var] = ''
            row['ip3_'+var] = ''
    
    # ip dist vars
    try:
        lat1=float(row['tmx_payer_proxy_ip_latitude'])
        lon1=float(row['tmx_payer_proxy_ip_longitude'])
        lat2=float(row['tmx_payee_proxy_ip_latitude'])
        lon2=float(row['tmx_payee_proxy_ip_longitude'])
        row['ip_dist_payer_payee_proxy']=geodist(lat1, lon1, lat2,  lon2)
    except:
        row['ip_dist_payer_payee_proxy']=""
    
    try:
        lat1=float(row['tmx_payer_true_ip_latitude'])
        lon1=float(row['tmx_payer_true_ip_longitude'])
        lat2=float(row['tmx_payee_true_ip_latitude'])
        lon2=float(row['tmx_payee_true_ip_longitude'])
        row['ip_dist_payer_payee_true']=geodist(lat1, lon1, lat2,  lon2)
    except:
        row['ip_dist_payer_payee_true']=""
    
    try:
        lat1=float(row['tmx_payer_proxy_ip_latitude'])
        lon1=float(row['tmx_payer_proxy_ip_longitude'])
        lat2=float(row['tmx_payer_true_ip_latitude'])
        lon2=float(row['tmx_payer_true_ip_longitude'])
        row['ip_dist_payer_proxy_true']=geodist(lat1, lon1, lat2,  lon2)
    except:
        row['ip_dist_payer_proxy_true']=""
    
    try:
        lat1=float(row['tmx_payee_proxy_ip_latitude'])
        lon1=float(row['tmx_payee_proxy_ip_longitude'])
        lat2=float(row['tmx_payee_true_ip_latitude'])
        lon2=float(row['tmx_payee_true_ip_longitude'])
        row['ip_dist_payee_proxy_true']=geodist(lat1, lon1, lat2,  lon2)
    except:
        row['ip_dist_payee_proxy_true']=""
    
    
    # ppcmp vars
    for var in ppcmp_var_list:
        try:
            if row['tmx_payer_'+var].lower()==row['tmx_payee_'+var].lower():
                row['ppcmp_'+var] = 1
            else:
                row['ppcmp_'+var] = 0
        except:
            row['ppcmp_'+var] = ''
    
    # leven dist
    for var in leven_dist_var_list:
        try:
            if row['ppcmp_'+var] == 1:
                row['leven_dist_'+var]=0
            else:
                row['leven_dist_'+var]=levenshtein(row['tmx_payer_'+var].lower(), row['tmx_payee_'+var].lower())
        except:
            row['leven_dist_'+var] = ''
            
    # zip 3,4 
    for var in zip_var_list:
        try:
            row['zip3_'+var]=row[var][0:3]
            row['zip4_'+var]=row[var][0:4]
        except:
            row['zip3_'+var]=''
            row['zip4_'+var]=''
            
    # missing ind for date time difference vars
    
    
    return row

def feature_creation(input_file,output_file):
    
    # input output files
    infile=gzip.open(input_file,'rb')
    incsv=csv.DictReader(infile)
    
    outfile=gzip.open(output_file,'w')
    outcsv=csv.writer(outfile)
    
    # datetime var list
    time_var_filename='/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt/var_list_time_diff.csv'
    time_var_file=open(time_var_filename,'rU')
    time_var_csv=csv.reader(time_var_file)
    time_var_list=[]
    for row in time_var_csv:
        time_var_list.append(row[0])
    
    # ip var list
    ip_var_list=['signal_580',
    'tmx_payer_proxy_ip',
    'tmx_payee_proxy_ip',
    'tmx_payee_true_ip',
    'tmx_payer_true_ip']
    
    # ppcmp var list 
    ppcmp_var_filename='/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt/var_list_ppcmp.csv'
    ppcmp_var_file=open(ppcmp_var_filename,'rU')
    ppcmp_var_csv=csv.reader(ppcmp_var_file)
    ppcmp_var_list=[]
    for row in ppcmp_var_csv:
        ppcmp_var_list.append(row[0])
    
    # leven dist var list
    leven_dist_var_filename='/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt/var_list_leven_dist.csv'
    leven_dist_var_file=open(leven_dist_var_filename,'rU')
    leven_dist_var_csv=csv.reader(leven_dist_var_file)
    leven_dist_var_list=[]
    for row in leven_dist_var_csv:
        leven_dist_var_list.append(row[0])
    
    # zip var list
    zip_var_list=['tmx_payer_account_address_zip','tmx_payee_account_address_zip']
    
    
    # new header of output file
    header=incsv.fieldnames
    header=header+\
    ['df_'+var for var in time_var_list]+\
    ['df_signal159_payee_last_review','df_signal169_payer_last_review']+\
    ['payee_reviewed_ever', # review time signals
    'payee_reviewed_le_7d',
    'payee_reviewed_7_30d',
    'payee_reviewed_30_60d',       
    'payer_reviewed_ever',
    'payer_reviewed_le_7d',
    'payer_reviewed_7_30d',
    'payer_reviewed_30_60d',]+\
    ['ip2_'+var for var in ip_var_list]+\
    ['ip3_'+var for var in ip_var_list]+\
    ['ip_dist_payer_payee_proxy','ip_dist_payer_payee_true','ip_dist_payer_proxy_true','ip_dist_payee_proxy_true']+\
    ['ppcmp_'+var for var in ppcmp_var_list]+\
    ['leven_dist_'+var for var in leven_dist_var_list]+\
    ['zip3_'+var for var in zip_var_list]+\
    ['zip4_'+var for var in zip_var_list]+\
    ['md_df_'+var for var in time_var_list]+\
    ['md_df_signal159_payee_last_review','md_df_signal169_payer_last_review']
    
    
    # write header of output file
    outcsv.writerow([var for var in header])

    
    t0=time.time()
    nRow=0
    for row in incsv:
        
        feature_creation_one_row(row,time_var_list,ip_var_list,ppcmp_var_list,leven_dist_var_list,zip_var_list)
        
        outcsv.writerow([row[var] for var in header])
        
        
        if nRow%10000 == 0:
            print nRow,"row has been processed; time lapsed:",time.time()-t0
                   
        nRow+=1
    
    infile.close()
    outfile.close()


def feature_creation_helper(arg):
    feature_creation(arg[0],arg[1])
    #feature_creation(input_file,output_file)
    

work_dir='/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt/'


input_list = (
              [work_dir+"model_data_pmt_ins_ds.csv.gz",work_dir+"model_data_pmt_ins_ds_fc.csv.gz"],
              [work_dir+"model_data_pmt_oos_ds.csv.gz",work_dir+"model_data_pmt_oos_ds_fc.csv.gz"],
              [work_dir+"test_data_sept_pmt_ds.csv.gz",work_dir+"test_data_sept_pmt_ds_fc.csv.gz"],
              [work_dir+"test_data_oct_pmt_ds.csv.gz",work_dir+"test_data_oct_pmt_ds_fc.csv.gz"],
              [work_dir+"test_data_nov_pmt_ds.csv.gz",work_dir+"test_data_nov_pmt_ds_fc.csv.gz"],
              [work_dir+"test_data_dec_pmt_ds.csv.gz",work_dir+"test_data_dec_pmt_ds_fc.csv.gz"],
              )
            # Inputs: feature_creation(input_file,output_file)
pool = Pool(processes=3)
pool.map(feature_creation_helper, input_list)

'''

#===============================================================================
# Old non-parallel
#===============================================================================

# ins
input_file=work_dir+"model_data_pmt_ins_ds.csv.gz"
output_file=work_dir+"model_data_pmt_ins_ds_fc.csv.gz"
feature_creation(input_file,output_file)

# oos
input_file=work_dir+"model_data_pmt_oos_ds.csv.gz"
output_file=work_dir+"model_data_pmt_oos_ds_fc.csv.gz"
feature_creation(input_file,output_file)

# test1
input_file=work_dir+"test_data_sept_pmt_ds.csv.gz"
output_file=work_dir+"test_data_sept_pmt_ds_fc.csv.gz"
feature_creation(input_file,output_file)

# test2
input_file=work_dir+"test_data_oct_pmt_ds.csv.gz"
output_file=work_dir+"test_data_oct_pmt_ds_fc.csv.gz"
feature_creation(input_file,output_file)

# test3
input_file=work_dir+"test_data_nov_pmt_ds.csv.gz"
output_file=work_dir+"test_data_nov_pmt_ds_fc.csv.gz"
feature_creation(input_file,output_file)

# test4
input_file=work_dir+"test_data_dec_pmt_ds.csv.gz"
output_file=work_dir+"test_data_dec_pmt_ds_fc.csv.gz"
feature_creation(input_file,output_file)

'''

csv_EDD(work_dir+"model_data_pmt_ins_ds_fc.csv.gz")