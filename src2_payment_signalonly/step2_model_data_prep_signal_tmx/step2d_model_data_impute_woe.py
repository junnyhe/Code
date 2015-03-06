import csv
import gzip
import os
import sys
import datetime
import random
from numpy import *
sys.path.append("/home/junhe/fraud_model/Code/tools/csv_operations")
import csv_ops
from csv_ops import *
from csv_woe_cat import *
from csv_impute import *
from multiprocessing import Pool



def impute_replace_pickle_helper(arg):
    impute_replace_pickle(arg[0],arg[1],arg[2])
    #function inputs: impute_replace_pickle(work_dir,input_file,output_file)
    
    
def woe_assign_pickle_helper(arg):
    woe_assign_pickle(arg[0],arg[1],arg[2])
    #function inputs: woe_assign_pickle(work_dir,input_file,output_file)
    
work_dir='/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt_signalonly/' # everything should/will be in work_dir


################################################################################
# Perform imputation                                                           #
# 1. Calculate statistics, create missing value mapping                        #
################################################################################

ins_file="model_data_pmt_ins_ds.csv.gz" # in sample data used to calculate the stats
imp_median_var_list_file="var_list_impute_median.csv" # var list for imputing missing to median 
imp_zero_var_list_file="var_list_impute_zero.csv" # var list for imputeing missing to zero

impute_create_mapping(work_dir,ins_file,imp_median_var_list_file,imp_zero_var_list_file)


################################################################################
# Perform imputation                                                           #
# 2. Replace missing values                                                    #
################################################################################
input_list = (
              [work_dir,"model_data_pmt_ins_ds.csv.gz","model_data_pmt_ins_ds_imp.csv.gz"], 
              [work_dir,"model_data_pmt_oos_ds.csv.gz","model_data_pmt_oos_ds_imp.csv.gz"],
              [work_dir,"test_data_sept_pmt_ds.csv.gz","test_data_sept_pmt_ds_imp.csv.gz"],
              [work_dir,"test_data_oct_pmt_ds.csv.gz","test_data_oct_pmt_ds_imp.csv.gz"],
              [work_dir,"test_data_nov_pmt_ds.csv.gz","test_data_nov_pmt_ds_imp.csv.gz"],
              [work_dir,"test_data_dec_pmt_ds.csv.gz","test_data_dec_pmt_ds_imp.csv.gz"]
              )
            # Inputs: impute_replace_pickle(work_dir,input_file,output_file)
pool = Pool(processes=3)
pool.map(impute_replace_pickle_helper, input_list)


#csv_EDD(work_dir+'model_data_pmt_ins_ds_imp.csv.gz')

################################################################################
# Create WOE variables                                                         #
# 1. Calculate statistics, create WOE mapping                                  #
################################################################################

input_file='model_data_pmt_ins_ds_imp.csv.gz'
woe_var_list_file='var_list_woe.csv'

risk_table(work_dir, input_file, woe_var_list_file, target='target', smooth_num=0, target_value='1')

################################################################################
# Create WOE variables                                                         #
# 2. Create WOE variables                                                      #
################################################################################

input_list = (
              [work_dir,"model_data_pmt_ins_ds_imp.csv.gz","model_data_pmt_ins_ds_imp_woe.csv.gz"],
              [work_dir,"model_data_pmt_oos_ds_imp.csv.gz","model_data_pmt_oos_ds_imp_woe.csv.gz"],
              [work_dir,"test_data_sept_pmt_ds_imp.csv.gz","test_data_sept_pmt_ds_imp_woe.csv.gz"],
              [work_dir,"test_data_oct_pmt_ds_imp.csv.gz","test_data_oct_pmt_ds_imp_woe.csv.gz"],
              [work_dir,"test_data_nov_pmt_ds_imp.csv.gz","test_data_nov_pmt_ds_imp_woe.csv.gz"],
              [work_dir,"test_data_dec_pmt_ds_imp.csv.gz","test_data_dec_pmt_ds_imp_woe.csv.gz"]
              )
            # Inputs: woe_assign_pickle(work_dir,input_file,output_file)
pool = Pool(processes=3)
pool.map(woe_assign_pickle_helper, input_list)


################################################################################
# check data quality, determine modeling var list                              #
################################################################################

csv_EDD(work_dir+'model_data_pmt_ins_ds_imp_woe.csv.gz')


