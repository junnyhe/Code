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
def rule_optimizaiton(data,target,full_rule_num_list,metric_name):
    
    resultfile=open(result_file,'w')
    resultcsv=csv.writer(resultfile)
    resultcsv.writerow(['rule_num_to_add','keep_rule_list','remaining_rule_list','catch_cnt', 'refer_cnt', 'total_fraud', 'catch_rate', 'hit_rate', 'refer_rate', 'F_measure'])
    
    
    # rule selection

    
    candidate_rule_list=range(data.shape[1]) # rule list is number of index, not original rule number
    keep_rule_list=[]
    
    total_fraud = sum(target)
    total_payments = len(target)
    rule_decision_results_prev=array([0]*data.shape[0])
    results_prev=[0]*7
    
    while len(candidate_rule_list)>0:
        metric_list=[]
        results=[]
        rule_decision_results_list=[]
        for i,rule_ind in enumerate(candidate_rule_list):
            # update results if add a rule
            rule_decision_results=(rule_decision_results_prev+data[:,rule_ind])>0
            
            # calculate metrics
            refer_cnt=sum(rule_decision_results)
            catch_cnt=sum(rule_decision_results*target)
            
            catch_rate=catch_cnt/float(total_fraud)
            hit_rate=catch_cnt/float(refer_cnt)
            refer_rate=refer_cnt/float(total_payments )
            F_measure=2*hit_rate*catch_rate/float(hit_rate+catch_rate)
            
            catch_rate_delta=catch_rate-results_prev[3]
            hit_rate_delta=hit_rate-results_prev[4]
            metric1=catch_rate_delta+hit_rate_delta*10
            metric2=catch_rate_delta+hit_rate_delta
            metric3=catch_rate_delta+hit_rate_delta/3
            metric4=catch_rate_delta+hit_rate_delta/10
            metric5=catch_rate_delta+hit_rate_delta/100
            
            
            # stored for retrieval when find best rule to add
            rule_decision_results_list.append(rule_decision_results)
            results.append([catch_cnt, refer_cnt, total_fraud, catch_rate, hit_rate, refer_rate, F_measure])
            # collecting metric
            if metric_name=="F_measure":
                metric_list.append(F_measure)
            elif metric_name=="hit_rate":
                metric_list.append(hit_rate)
            elif metric_name=="catch_rate":
                metric_list.append(catch_rate)
            elif metric_name=="metric1":
                metric_list.append(metric1)
            elif metric_name=="metric2":
                metric_list.append(metric2)
            elif metric_name=="metric3":
                metric_list.append(metric3)
            elif metric_name=="metric4":
                metric_list.append(metric4)
            elif metric_name=="metric5":
                metric_list.append(metric5)
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
        print "catch_cnt, refer_cnt, total_fraud, catch_rate, hit_rate, refer_rate, F_measure"
        print results[best_rule_to_add_index]
        print "keep rule list:\n",keep_rule_num_list
        print "candidate rule list:\n",candidate_rule_num_list
        
        resultcsv.writerow([rule_num_to_add,keep_rule_num_list,candidate_rule_num_list]+results[best_rule_to_add_index])
        
        print [rule_num_to_add,keep_rule_list,candidate_rule_list]+results[best_rule_to_add_index]
    
    resultfile.close()
    
    

# load data to array
input_file="/fraud_model/Data/rule_optimization/rule_fire_and_review_results_all.csv.gz"
infile=gzip.open(input_file,'rb')
incsv=csv.reader(infile)
header=incsv.next()

nRow=0
data=[]
for row in incsv:
    data.append([int_conv(val,0) for val in row[1:]]) #exclude pr_id
    nRow+=1
    if nRow%100000 ==0:
        print nRow,' rows are loaded'
        

data=array(data)

target_state=data[:,-2]
target_deny=data[:,-1]
data=data[:,:-2] # exclude targets

full_rule_num_list=header[1:-2]
full_rule_num_list=[int(rule.replace('rule_','')) for rule in full_rule_num_list]


'''
metric_name="F_measure"
result_file="/fraud_model/Results/rule_optimization/rule_optimization_sequence_target_deny_"+metric_name+".csv"
rule_optimizaiton(data,target_deny,full_rule_num_list,metric_name)

metric_name="hit_rate"
result_file="/fraud_model/Results/rule_optimization/rule_optimization_sequence_target_deny_"+metric_name+".csv"
rule_optimizaiton(data,target_deny,full_rule_num_list,metric_name)

metric_name="catch_rate"
result_file="/fraud_model/Results/rule_optimization/rule_optimization_sequence_target_deny_"+metric_name+".csv"
rule_optimizaiton(data,target_deny,full_rule_num_list,metric_name)


metric_name="F_measure"
result_file="/fraud_model/Results/rule_optimization/rule_optimization_sequence_target_state_"+metric_name+".csv"
rule_optimizaiton(data,target_deny,full_rule_num_list,metric_name)

metric_name="hit_rate"
result_file="/fraud_model/Results/rule_optimization/rule_optimization_sequence_target_state_"+metric_name+".csv"
rule_optimizaiton(data,target_deny,full_rule_num_list,metric_name)

metric_name="catch_rate"
result_file="/fraud_model/Results/rule_optimization/rule_optimization_sequence_target_state_"+metric_name+".csv"
rule_optimizaiton(data,target_deny,full_rule_num_list,metric_name)
'''

metric_name="metric1"
result_file="/fraud_model/Results/rule_optimization/rule_optimization_sequence_target_deny_"+metric_name+".csv"
rule_optimizaiton(data,target_deny,full_rule_num_list,metric_name)

metric_name="metric1"
result_file="/fraud_model/Results/rule_optimization/rule_optimization_sequence_target_state_"+metric_name+".csv"
rule_optimizaiton(data,target_deny,full_rule_num_list,metric_name)

metric_name="metric2"
result_file="/fraud_model/Results/rule_optimization/rule_optimization_sequence_target_deny_"+metric_name+".csv"
rule_optimizaiton(data,target_deny,full_rule_num_list,metric_name)

metric_name="metric2"
result_file="/fraud_model/Results/rule_optimization/rule_optimization_sequence_target_state_"+metric_name+".csv"
rule_optimizaiton(data,target_deny,full_rule_num_list,metric_name)

metric_name="metric3"
result_file="/fraud_model/Results/rule_optimization/rule_optimization_sequence_target_deny_"+metric_name+".csv"
rule_optimizaiton(data,target_deny,full_rule_num_list,metric_name)

metric_name="metric3"
result_file="/fraud_model/Results/rule_optimization/rule_optimization_sequence_target_state_"+metric_name+".csv"
rule_optimizaiton(data,target_deny,full_rule_num_list,metric_name)

metric_name="metric4"
result_file="/fraud_model/Results/rule_optimization/rule_optimization_sequence_target_deny_"+metric_name+".csv"
rule_optimizaiton(data,target_deny,full_rule_num_list,metric_name)

metric_name="metric4"
result_file="/fraud_model/Results/rule_optimization/rule_optimization_sequence_target_state_"+metric_name+".csv"
rule_optimizaiton(data,target_deny,full_rule_num_list,metric_name)

metric_name="metric5"
result_file="/fraud_model/Results/rule_optimization/rule_optimization_sequence_target_deny_"+metric_name+".csv"
rule_optimizaiton(data,target_deny,full_rule_num_list,metric_name)

metric_name="metric5"
result_file="/fraud_model/Results/rule_optimization/rule_optimization_sequence_target_state_"+metric_name+".csv"
rule_optimizaiton(data,target_deny,full_rule_num_list,metric_name)



