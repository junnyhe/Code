import pandas as pd
import numpy as np
import copy as cp

global total_fraud_cnt, total_cnt, direction
direction=1 # = 1 for 'pmt' or 2 for 'wd'
type='pmt' # = 'pmt' or 'wd'


out_dir = '/fraud_model/Results/model_rule_optimization/'
# load data
df=pd.read_csv('/fraud_model/Data/model_rule_optimization/rule_score_'+type+'_20150409_20150415.csv')

# fill Nan
df.blacklist_reason.fillna("Good",inplace=True)
df.fillna(0,inplace=True)

# filter data by direction and blacklist reason in (Fraud, Good)
df = df[(df.direction == direction) * ((df.blacklist_reason =='Fraud') + (df.blacklist_reason =='Good'))]

# get rule, score_rule list
header = df.columns.values
rule_list = [var_name for var_name in header if "rule_" in var_name and var_name not in ("rule_463","rule_468","rule_123","rule_126")]
score_rule_list = [var_name for var_name in header if "score_ge_" in var_name]

rule_list_base= cp.deepcopy(rule_list)
rule_list_base.remove('rule_130')
rule_list_base.remove('rule_216')
rule_list_base.remove('rule_12')
rule_list_base.remove('rule_21')
rule_list_base.remove('rule_127')
rule_list_base.remove('rule_36')
rule_list_base.remove('rule_30')
rule_list_base.append('score_ge_45')

rule_decision_base = np.array([0]*len(df))

for rule in rule_list_base:
    rule_decision_base = 1*((rule_decision_base+df[rule])>0)


test_rule_list = ['rule_130',
                'rule_216',
                'rule_12',
                'rule_21',
                'rule_127']
print sum((df.blacklist_reason=='Fraud') * rule_decision_base), sum(rule_decision_base)
for test_rule in test_rule_list:
    rule_decision_curr = ((rule_decision_base + df[test_rule])>0)*1
    print "\n\nadditional fraud catched by",test_rule,'are:'
    print df.payment_request_id[(rule_decision_base==0) *(rule_decision_curr==1) * (df.blacklist_reason=='Fraud')]
    print "Fraud:",sum((df.blacklist_reason=='Fraud') * rule_decision_curr), "Refer:", sum(rule_decision_curr)
    rule_decision_base = rule_decision_curr
    
