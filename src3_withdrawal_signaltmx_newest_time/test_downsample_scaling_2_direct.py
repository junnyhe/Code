import gzip
import csv
import random
import sys
sys.path.append("/home/junhe/fraud_model/Code/tools/model_tools")
from load_data import *
from numpy import *

def downsample_filter(input_file,output_file, downsamle_fieldname, downsample_field_equal_value, downsample_frac):
    
    print "Downsampling file:",input_file
    # get input
    infile=gzip.open(input_file,'rb')
    incsv=csv.DictReader(infile)
    header_out=incsv.fieldnames
    header_out=['payment_request_id','signal_1','signal_12','signal_17','signal_18','signal_19','signal_24','manual_review','target','target2']
    
    outfile=gzip.open(output_file,'w')
    outcsv=csv.writer(outfile)
    outcsv.writerow(header_out)
    
    random.seed(1)
    nRow=0
    for row in incsv:
        if not (row['target']=='1' and row['target2'] !='1'):# additional filter (exclude nonfraud blacklisted):
            
            for key in header_out:
                try:
                    row[key]=float(row[key])
                except:
                    row[key]=0
            
            if float(row[downsamle_fieldname])==float(downsample_field_equal_value): #downsample good
                
                if random.random() <  downsample_frac:
                    outcsv.writerow([row[key] for key in header_out])
            else: # keep bad
                outcsv.writerow([row[key] for key in header_out])
            
            nRow+=1
            if nRow%10000 ==0:
                print nRow," rows are processed"
            
            #else: # exclusion
            #    print row['target'], row['target2'], row['blacklist_reason']
    
    print "In total,",nRow," rows are processed"
    


good_downsample_rate = 1 #used to scale back hit rate
in_dir='/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v3wd_newest_time/'
out_dir='/home/junhe/fraud_model/Data/test/'
input_file=in_dir+"model_data_wd_oos.csv.gz"
output_file=out_dir+"model_data_wd_oos_ds.csv.gz"
downsample_filter(input_file,output_file, downsamle_fieldname='target', downsample_field_equal_value='0', downsample_frac=good_downsample_rate)


# load down sampled data
data_dir='/home/junhe/fraud_model/Data/test/'

test_data_file=data_dir+'model_data_wd_oos_ds.csv.gz'
var_list_filename=data_dir+'test_vars.csv'
target_name='target'
key_name='payment_request_id'
tag_name='manual_review'


X,y,key,tag = load_data_with_key_tag_fast(test_data_file, var_list_filename, target_name, key_name, tag_name)
scale_factor = (1-y)*(1/good_downsample_rate)+y

print y,tag,scale_factor

# find rates of rule
catch_rate_rule = sum(y*tag*scale_factor)/sum(y*scale_factor) # fraud found by rule tag / total fraud
hit_rate_rule = sum(y*tag*scale_factor)/sum(tag*scale_factor) # fraud found by rule tag / total referred by rule
refer_rate_rule = sum(tag*scale_factor)/sum(scale_factor) # fraud found by rule tag / total referred by rule

print len(y),sum(y),sum(tag),catch_rate_rule, hit_rate_rule, refer_rate_rule



'''
good_downsample_rate = .2 #used to scale back hit rate
# load down sampled data
data_dir='/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v3wd_newest_time/'

test_data_file=data_dir+'model_data_wd_oos_ds_rcind_fc_imp_woe.csv.gz'
var_list_filename='/home/junhe/fraud_model/Data/test/test_vars.csv'
target_name='target'
key_name='payment_request_id'
tag_name='manual_review'


X,y,key,tag = load_data_with_key_tag_fast(test_data_file, var_list_filename, target_name, key_name, tag_name)
scale_factor = (1-y)*(1/good_downsample_rate)+y

print y,tag,scale_factor

# find rates of rule
catch_rate_rule = sum(y*tag*scale_factor)/sum(y*scale_factor) # fraud found by rule tag / total fraud
hit_rate_rule = sum(y*tag*scale_factor)/sum(tag*scale_factor) # fraud found by rule tag / total referred by rule
refer_rate_rule = sum(tag*scale_factor)/sum(scale_factor) # fraud found by rule tag / total referred by rule

print len(y),sum(y),sum(tag),catch_rate_rule, hit_rate_rule, refer_rate_rule
'''
