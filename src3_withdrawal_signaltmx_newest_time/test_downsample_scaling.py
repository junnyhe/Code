import sys
sys.path.append("/fraud_model/Code/tools/model_tools")
from load_data import *
import random
from numpy import *

data_dir='/fraud_model/Data/Model_Data_Signal_Tmx_v3wd_newest_time/'
result_dir='/fraud_model/Results/Model_Results_Signal_Tmx_v3wd_woeSmth=0_newest_time/'

test_data_file=data_dir+'model_data_wd_oos_ds_rcind_fc_imp_woe.csv.gz'
var_list_filename=result_dir+'model_var_list_signal_rc_tmxrc_ind.csv'
target_name='target'
key_name='payment_request_id'
tag_name='manual_review'
X,y,key,tag = load_data_with_key_tag_fast(test_data_file, var_list_filename, target_name, key_name, tag_name)

y_tag=zip(list(y),list(tag))

ds_rate=0.1
y_tag_ds = array([row for row in y_tag if (row[0]==0 and random.random()<ds_rate) or row[0]==1])

yN=y_tag_ds[:,0]
tagN=y_tag_ds[:,1]
scale_factor = (1-yN)*(1/ds_rate)+yN
catch_rate_rule = sum(yN*tagN*scale_factor)/sum(yN*scale_factor) # fraud found by rule tag / total fraud
hit_rate_rule = sum(yN*tagN*scale_factor)/sum(tagN*scale_factor) # fraud found by rule tag / total referred by rule
refer_rate_rule = sum(tagN*scale_factor)/sum(scale_factor) # fraud found by rule tag / total referred by rule
print catch_rate_rule , hit_rate_rule, refer_rate_rule



