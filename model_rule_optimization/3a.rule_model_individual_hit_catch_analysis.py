import pandas as pd
import numpy as np

global total_fraud_cnt, total_cnt, direction
direction=1 # = 1 for 'pmt' or 2 for 'wd'
type='pmt' # = 'pmt' or 'wd'


out_dir = '/fraud_model/Results/model_rule_optimization/'
# load data
df=pd.read_csv('/fraud_model/Data/model_rule_optimization/rule_score_'+type+'_20150409_20150415.csv')

# fill Nan
df.blacklist_reason.fillna("Good",inplace=True)
df.fillna(0,inplace=True)

# get rule, score_rule list
header = df.columns.values
rule_list = [var_name for var_name in header if "rule_" in var_name and var_name not in ("rule_463","rule_468")]
score_rule_list = [var_name for var_name in header if "score_ge_" in var_name]

# get total cnt and total fraud cnt
tmp = pd.pivot_table(df, values='payment_request_id', index=['direction'], columns=['blacklist_reason'], aggfunc=len)
total_fraud_cnt = tmp.ix[direction].Fraud
total_good_cnt = tmp.ix[direction].Good
total_cnt = total_fraud_cnt + total_good_cnt

def analyze_hit_catch(rule):
    tmp = pd.pivot_table(df, values='payment_request_id', index=['direction',rule], columns=['blacklist_reason'], aggfunc=len)
    tmp.fillna(0,inplace=True)
    try:
        fraud_cnt = tmp.ix[direction,1].Fraud
    except:
        fraud_cnt = 0
    try:
        good_cnt = tmp.ix[direction,1].Good
    except:
        good_cnt = 0
    if fraud_cnt+good_cnt !=0:
        hit_rate = fraud_cnt/float(fraud_cnt+good_cnt)
    else: hit_rate = np.NaN
    catch_rate = fraud_cnt/float(total_fraud_cnt)
    refer_rate = (fraud_cnt+good_cnt)/float(total_cnt)
    return fraud_cnt,good_cnt,hit_rate,catch_rate,refer_rate

# analyze hit catch for different score cutoff
results = []
for rule in score_rule_list:
    fraud_cnt,good_cnt,hit_rate,catch_rate,refer_rate = analyze_hit_catch(rule)
    results.append([fraud_cnt,good_cnt,hit_rate,catch_rate,refer_rate])
results_df = pd.DataFrame(np.array(results),index=score_rule_list,columns=['fraud_cnt','good_cnt','hit_rate','catch_rate','refer_rate'])
results_df.to_csv(out_dir+type+'_score_cutoff_hit_catch.csv')

# analyze hit catch for different rules
results = []
for rule in rule_list:
    fraud_cnt,good_cnt,hit_rate,catch_rate,refer_rate = analyze_hit_catch(rule)
    results.append([fraud_cnt,good_cnt,hit_rate,catch_rate,refer_rate])
results_df = pd.DataFrame(np.array(results),index=rule_list,columns=['fraud_cnt','good_cnt','hit_rate','catch_rate','refer_rate'])
results_df.to_csv(out_dir+type+'_rule_hit_catch.csv')






