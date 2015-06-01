import csv
import gzip
import os
import sys
import datetime
import random
from numpy import *
sys.path.append("/Users/junhe/Box Sync/Documents/workspace/fraud_model/tools/csv_operations")
import csv_ops
from csv_ops import *
from csv_woe_cat import *
from csv_impute import *



work_dir='/Users/junhe/Box Sync/Documents/Data/Model_Data_Signal_Tmx_v2wd/' # everything should/will be in work_dir

#csv_EDD(work_dir+'model_data_wd_ins_ds_fc.csv.gz')

################################################################################
# Perform imputation                                                           #
# 1. Calculate statistics, create missing value mapping                        #
################################################################################

ins_file="model_data_wd_ins_ds_fc.csv.gz" # in sample data used to calculate the stats
imp_median_var_list_file="var_list_impute_median.csv" # var list for imputing missing to median 
imp_zero_var_list_file="var_list_impute_zero.csv" # var list for imputeing missing to zero

impute_create_mapping(work_dir,ins_file,imp_median_var_list_file,imp_zero_var_list_file)


################################################################################
# Perform imputation                                                           #
# 2. Replace missing values                                                    #
################################################################################
# ins
input_file="model_data_wd_ins_ds_fc.csv.gz"
output_file="model_data_wd_ins_ds_fc_imp.csv.gz"
impute_replace_pickle(work_dir,input_file,output_file)

# oos
input_file="model_data_wd_oos_ds_fc.csv.gz"
output_file="model_data_wd_oos_ds_fc_imp.csv.gz"
impute_replace_pickle(work_dir,input_file,output_file)

# test1
input_file="test_data_sept_wd_ds_fc.csv.gz"
output_file="test_data_sept_wd_ds_fc_imp.csv.gz"
impute_replace_pickle(work_dir,input_file,output_file)

# test2
input_file="test_data_oct_wd_ds_fc.csv.gz"
output_file="test_data_oct_wd_ds_fc_imp.csv.gz"
impute_replace_pickle(work_dir,input_file,output_file)

# test3
input_file="test_data_nov_wd_ds_fc.csv.gz"
output_file="test_data_nov_wd_ds_fc_imp.csv.gz"
impute_replace_pickle(work_dir,input_file,output_file)

# test4
input_file="test_data_dec_wd_ds_fc.csv.gz"
output_file="test_data_dec_wd_ds_fc_imp.csv.gz"
impute_replace_pickle(work_dir,input_file,output_file)


#csv_EDD(work_dir+'model_data_wd_ins_ds_fc_imp.csv.gz')

################################################################################
# Create WOE variables                                                         #
# 1. Calculate statistics, create WOE mapping                                  #
################################################################################

input_file='model_data_wd_ins_ds_fc_imp.csv.gz'
woe_var_list_file='var_list_woe.csv'

risk_table(work_dir, input_file, woe_var_list_file, target='target', smooth_num=500, target_value='1')

################################################################################
# Create WOE variables                                                         #
# 2. Create WOE variables                                                   #
################################################################################


# ins
input_file='model_data_wd_ins_ds_fc_imp.csv.gz'
output_file='model_data_wd_ins_ds_fc_imp_woe.csv.gz'
woe_assign_pickle(work_dir, input_file, output_file)

# oos
input_file='model_data_wd_oos_ds_fc_imp.csv.gz'
output_file='model_data_wd_oos_ds_fc_imp_woe.csv.gz'
woe_assign_pickle(work_dir, input_file, output_file)

# test1
input_file="test_data_sept_wd_ds_fc_imp.csv.gz"
output_file="test_data_sept_wd_ds_fc_imp_woe.csv.gz"
woe_assign_pickle(work_dir, input_file, output_file)

# test2
input_file="test_data_oct_wd_ds_fc_imp.csv.gz"
output_file="test_data_oct_wd_ds_fc_imp_woe.csv.gz"
woe_assign_pickle(work_dir, input_file, output_file)

# test3
input_file="test_data_nov_wd_ds_fc_imp.csv.gz"
output_file="test_data_nov_wd_ds_fc_imp_woe.csv.gz"
woe_assign_pickle(work_dir, input_file, output_file)

# test4
input_file="test_data_dec_wd_ds_fc_imp.csv.gz"
output_file="test_data_dec_wd_ds_fc_imp_woe.csv.gz"
woe_assign_pickle(work_dir, input_file, output_file)


################################################################################
# check data quality, determine modeling var list                              #
################################################################################

#csv_EDD(work_dir+'model_data_wd_ins_ds_fc_imp_woe.csv.gz')


