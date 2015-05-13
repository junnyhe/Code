import csv
import gzip
import os
import datetime
from numpy import *


def int_conv(n,default=0):
    try:
        return int(n)
    except:
        return default
    
    
# rule optimization
def rule_optimizaiton(data,target,amount,full_rule_num_list,metric_name,result_file):
    
    resultfile=open(result_file,'w')
    resultcsv=csv.writer(resultfile)
    resultcsv.writerow(['rule_num_to_add','keep_rule_list','remaining_rule_list','catch_cnt', 'refer_cnt', 'catch_amt', 'refer_amt','total_fraud_cnt', 'catch_rate', 'hit_rate', 'refer_rate', 'F_measure'])
    
    
    # rule selection

    
    candidate_rule_list=range(data.shape[1]) # rule list is number of index, not original rule number
    keep_rule_list=[]
    
    total_fraud_cnt = sum(target)
    total_fraud_amt = sum(target*amount)
    total_payment_cnt = len(target)
    total_payment_amt = sum(amount)
    rule_decision_results_prev=array([0]*data.shape[0])
    results_prev=[0]*9
    
    while len(candidate_rule_list)>0:
        metric_list=[]
        results=[]
        rule_decision_results_list=[]
        for i,rule_ind in enumerate(candidate_rule_list):
            # update results if add a rule
            rule_decision_results=(rule_decision_results_prev+data[:,rule_ind])>0 # logic "or" relation
            
            # calculate metrics
            refer_cnt=sum(rule_decision_results)
            refer_amt=sum(rule_decision_results*amount)
            catch_cnt=sum(rule_decision_results*target)
            catch_amt=sum(rule_decision_results*target*amount)
            
            catch_rate=catch_cnt/float(total_fraud_cnt)
            catch_amt_rate=catch_amt/float(total_fraud_amt)
            hit_rate=catch_cnt/float(refer_cnt)
            hit_amt_rate=catch_amt/float(refer_amt)
            refer_rate=refer_cnt/float(total_payment_cnt )
            refer_amt_rate=refer_amt/float(total_payment_cnt )
            F_measure=2*hit_rate*catch_rate/float(hit_rate+catch_rate)
            
            
            
            # stored for retrieval when find best rule to add
            rule_decision_results_list.append(rule_decision_results)
            results.append([catch_cnt, refer_cnt, catch_amt, refer_amt, total_fraud_cnt, catch_rate, hit_rate, refer_rate, F_measure])
            # collecting metric
            if metric_name=="F_measure":
                metric_list.append(F_measure)
            elif metric_name=="hit_rate":
                metric_list.append(hit_rate)
            elif metric_name=="catch_rate":
                metric_list.append(catch_rate)
            else:
                print "no valid metric name specified, will use F_measure"
                metric_list.append(F_measure)
            
        # find best rule by metric
        best_rule_to_add_index = metric_list.index(max([metric for metric in metric_list if not isnan(metric)]))
        # update the rule combination results for adding next rule
        rule_decision_results_prev=rule_decision_results_list[best_rule_to_add_index]
        results_prev=results[best_rule_to_add_index]
        
        
        # prepare output reuslts
        #print "best rule location is",best_rule_to_add_index, 
        rule_index_to_add = candidate_rule_list[best_rule_to_add_index]
        rule_num_to_add = full_rule_num_list[candidate_rule_list[best_rule_to_add_index]]
        
        keep_rule_list.append(candidate_rule_list[best_rule_to_add_index])
        candidate_rule_list.remove(candidate_rule_list[best_rule_to_add_index])
        
        keep_rule_num_list=list(array(full_rule_num_list)[keep_rule_list])
        candidate_rule_num_list=list(array(full_rule_num_list)[candidate_rule_list])
        
        print "\nbest rule index is",rule_index_to_add, "\nbest rule num is",rule_num_to_add
        print "catch_cnt, refer_cnt, total_fraud_cnt, catch_rate, hit_rate, refer_rate, F_measure"
        print results[best_rule_to_add_index]
        print "keep rule list:\n",keep_rule_num_list
        print "candidate rule list:\n",candidate_rule_num_list
        
        resultcsv.writerow([rule_num_to_add,keep_rule_num_list,candidate_rule_num_list]+results[best_rule_to_add_index])
        
        print [rule_num_to_add,keep_rule_list,candidate_rule_list]+results[best_rule_to_add_index]
    
    resultfile.close()


def optimize_rule_w_score(score_ind_list):

    print "optimizing ",score_ind_list
    
    # prepare rule name list for input and results
    full_rule_num_list=[int(rule.replace('rule_','')) for rule in rule_list]
    full_rule_num_list = full_rule_num_list+score_ind_list
    
    # load data to array
    infile=open(input_file,'rb')
    incsv=csv.DictReader(infile)
    
    var_list = rule_list + score_ind_list + ['amount','target']
    
    nRow=0
    data=[]
    for row in incsv:
    
        if int(row['direction']) == direction and (row['blacklist_reason'] in ("Fraud","")):
            row['target']=0
            if row['blacklist_reason'] =="Fraud":
                row['target']=1
             
            data.append([int_conv(row[var],0) for var in var_list]) #exclude pr_id
        nRow+=1
        if nRow%10000 ==0:
            print nRow,' rows are loaded'
            
    
    data=array(data)
    
    target=data[:,-1]
    amount=data[:,-2]
    data=data[:,:-2] # exclude targets
    
    
    if len(score_ind_list)==1:
        score_ind_name_in_file = score_ind_list[0]
    elif len(score_ind_list)==0:
        score_ind_name_in_file = 'no_model'
    else:
        score_ind_name_in_file = "score_ind_"+str(len(score_ind_list))
    
    metric_name="F_measure"
    result_file=out_dir+"rule_optimization_sequence_"+type+"_"+metric_name+score_ind_name_in_file+".csv"
    rule_optimizaiton(data,target,amount,full_rule_num_list,metric_name,result_file)
    
    metric_name="hit_rate"
    result_file=out_dir+"rule_optimization_sequence_"+type+"_"+metric_name+score_ind_name_in_file+".csv"
    rule_optimizaiton(data,target,amount,full_rule_num_list,metric_name,result_file)
    
    metric_name="catch_rate"
    result_file=out_dir+"rule_optimization_sequence_"+type+"_"+metric_name+score_ind_name_in_file+".csv"
    rule_optimizaiton(data,target,amount,full_rule_num_list,metric_name,result_file)



out_dir="/home/junhe/fraud_model/Results/model_rule_optimization/pmt/"
type='pmt'
direction=1
'''
out_dir="/home/junhe/fraud_model/Results/model_rule_optimization/wd/"
type='wd'
direction=2
'''

# load header and get rule list
input_file="/home/junhe/fraud_model/Data/model_rule_optimization/rule_score_"+type+"_20150409_20150415.csv"
infile=open(input_file,'rb')
incsv=csv.reader(infile)
header=incsv.next()
infile.close()

rule_list = [var_name for var_name in header if "rule_" in var_name and var_name not in ("rule_463","rule_468","rule_123","rule_126")]
score_rule_list = [var_name for var_name in header if "score_ge_" in var_name]




# optimize for individual score cutoffs
for score_rule in score_rule_list:
    optimize_rule_w_score([score_rule])


# optimize for all score cutoffs
optimize_rule_w_score(score_rule_list)


# optimize for no model just rules
optimize_rule_w_score([])






  
    
