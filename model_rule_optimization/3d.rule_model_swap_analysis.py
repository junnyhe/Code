import pandas as pd
import numpy as np
import copy as cp

global df, direction
direction=1 # = 1 for 'pmt' or 2 for 'wd'
type='pmt' # = 'pmt' or 'wd'

def create_refer_ind(rule_set,refer_ind_name):
    df[refer_ind_name] = np.array([0]*len(df))
    for rule in rule_set:
        df[refer_ind_name]= ((df[refer_ind_name] + df[rule])>0)*1


def swap_analysis(rule_list_base,rule_list_remove,rule_list_add,result_file):
    
    # prepare refer ind based on old rules
    refer_ind_name = 'refer_ind_original'
    rule_set = rule_list_base
    create_refer_ind(rule_set,refer_ind_name)
    
    # prepare refer ind based on new rules (after add remove)
    refer_ind_name = 'refer_ind_new'
    rule_set = cp.deepcopy(rule_list_base)
    for rule in rule_list_remove: # update fules based on remove, add list
        rule_set.remove(rule)
    for rule in rule_list_add:
        rule_set.append(rule)
    create_refer_ind(rule_set,refer_ind_name)
    
    # calculate pivot table, and write to disk
    table = pd.pivot_table(df,values=['n','amount'],index=['refer_ind_original','refer_ind_new'],columns=['blacklist_reason'], aggfunc=np.sum)
    print table
    table.to_csv(result_file)
    
    # get swap-in and swap-out count
    try:
        swap_in_fraud_n = table.ix[0,1]['n','Fraud']
    except:
        swap_in_fraud_n =0
    try:   
        swap_in_good_n  = table.ix[0,1]['n','Good']
    except:
        swap_in_good_n = 0
    try:
        swap_out_fraud_n = table.ix[1,0]['n','Fraud']
    except:
        swap_out_fraud_n = 0
    try:
        swap_out_good_n  = table.ix[1,0]['n','Good']
    except:
         swap_out_good_n =0
    print "swap_in fraud",swap_in_fraud_n, "; swap_out fraud", swap_out_fraud_n,"\nswap_in good",swap_in_good_n, "; swap_out good", swap_out_good_n
    
    # get swap-in and swap-out amount
    try:
        swap_in_fraud_amt = table.ix[0,1]['amount','Fraud']
    except:
        swap_in_fraud_amt =0
    try:   
        swap_in_good_amt  = table.ix[0,1]['amount','Good']
    except:
        swap_in_good_amt = 0
    try:
        swap_out_fraud_amt = table.ix[1,0]['amount','Fraud']
    except:
        swap_out_fraud_amt = 0
    try:
        swap_out_good_amt  = table.ix[1,0]['amount','Good']
    except:
         swap_out_good_amt =0
    print "\nswap_in fraud amt",swap_in_fraud_amt,"; swap_out fraud amt", swap_out_fraud_amt, "\nswap_in good amt",swap_in_good_amt, "; swap_out good amt", swap_out_good_amt
    

############# load and preapre data #####################
out_dir = '/fraud_model/Results/model_rule_optimization/swap_analysis/'
# load data
df=pd.read_csv('/fraud_model/Data/model_rule_optimization/rule_score_'+type+'_20150409_20150415.csv')

# fill Nan
df.blacklist_reason.fillna("Good",inplace=True)
df.fillna(0,inplace=True)

# filter data by direction and blacklist reason in (Fraud, Good)
df = df[(df.direction == direction) * ((df.blacklist_reason =='Fraud') + (df.blacklist_reason =='Good'))]

# add one column for cnt
df['n'] = np.array([1]*len(df))

# get rule, score_rule list
header = df.columns.values
rule_list = [var_name for var_name in header if "rule_" in var_name and var_name not in ("rule_463","rule_468","rule_123","rule_126")]
score_rule_list = [var_name for var_name in header if "score_ge_" in var_name]


############# apply analysis functions #####################
score_rule_name = 'score_ge_40'
rule_list_base = rule_list
result_file = out_dir+'swap_analysis_'+score_rule_name+'.csv'
rule_list_remove = ['rule_130','rule_216','rule_21']
rule_list_add = [score_rule_name]
swap_analysis(rule_list_base,rule_list_remove,rule_list_add,result_file)


score_rule_name = 'score_ge_45'
rule_list_base = rule_list
result_file = out_dir+'swap_analysis_'+score_rule_name+'.csv'
rule_list_remove = ['rule_130','rule_216','rule_21']
rule_list_add = [score_rule_name]
swap_analysis(rule_list_base,rule_list_remove,rule_list_add,result_file)


score_rule_name = 'score_ge_50'
rule_list_base = rule_list
result_file = out_dir+'swap_analysis_'+score_rule_name+'.csv'
rule_list_remove = ['rule_130','rule_216','rule_21']
rule_list_add = [score_rule_name]
swap_analysis(rule_list_base,rule_list_remove,rule_list_add,result_file)


score_rule_name = 'score_ge_55'
rule_list_base = rule_list
result_file = out_dir+'swap_analysis_'+score_rule_name+'.csv'
rule_list_remove = ['rule_130','rule_216','rule_21']
rule_list_add = [score_rule_name]
swap_analysis(rule_list_base,rule_list_remove,rule_list_add,result_file)


score_rule_name = 'score_ge_60'
rule_list_base = rule_list
result_file = out_dir+'swap_analysis_'+score_rule_name+'.csv'
rule_list_remove = ['rule_130','rule_216','rule_21']
rule_list_add = [score_rule_name]
swap_analysis(rule_list_base,rule_list_remove,rule_list_add,result_file)

score_rule_name = 'score_ge_65'
rule_list_base = rule_list
result_file = out_dir+'swap_analysis_'+score_rule_name+'.csv'
rule_list_remove = ['rule_130','rule_216','rule_21']
rule_list_add = [score_rule_name]
swap_analysis(rule_list_base,rule_list_remove,rule_list_add,result_file)


score_rule_name = 'score_ge_70'
rule_list_base = rule_list
result_file = out_dir+'swap_analysis_'+score_rule_name+'.csv'
rule_list_remove = ['rule_130','rule_216','rule_21']
rule_list_add = [score_rule_name]
swap_analysis(rule_list_base,rule_list_remove,rule_list_add,result_file)

score_rule_name = 'score_ge_75'
rule_list_base = rule_list
result_file = out_dir+'swap_analysis_'+score_rule_name+'.csv'
rule_list_remove = ['rule_130','rule_216','rule_21']
rule_list_add = [score_rule_name]
swap_analysis(rule_list_base,rule_list_remove,rule_list_add,result_file)





