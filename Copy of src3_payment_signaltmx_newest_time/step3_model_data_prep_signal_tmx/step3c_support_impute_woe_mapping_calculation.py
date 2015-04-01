import csv
import gzip
import os
import sys
import time
import datetime
import random
from numpy import *
sys.path.append("/home/junhe/fraud_model/Code/tools/csv_operations")
import csv_ops
from csv_ops import *
from csv_woe_cat import *
from csv_impute import *
from multiprocessing import Pool



def impute_replace_woe_assign_helper(arg):
    impute_replace_woe_assign(arg[0],arg[1])

    
work_dir='/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v3pmt_newest_time/' # everything should/will be in work_dir


#===============================================================================
# # prepare missing value mapping                        
#===============================================================================

ins_file="model_data_pmt_ins_ds_rcind_fc.csv.gz" # in sample data used to calculate the stats
imp_median_var_list_file="var_list_impute_median.csv" # var list for imputing missing to median 
imp_zero_var_list_file="var_list_impute_zero.csv" # var list for imputeing missing to zero

impute_create_mapping(work_dir,ins_file,imp_median_var_list_file,imp_zero_var_list_file)


#===============================================================================
# # Prepare WOE mapping                                  
#===============================================================================

input_file='model_data_pmt_ins_ds_rcind_fc.csv.gz'
woe_var_list_file='var_list_woe.csv'

risk_table(work_dir, input_file, woe_var_list_file, target='target', smooth_num=0, target_value='1')
