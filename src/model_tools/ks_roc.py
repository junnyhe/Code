import random
import copy
import math
from numpy import *
    
def ks_roc(tgts, score):
    
    # sanity check
    if len(tgts) == len(score):
        print 'KS input data size', len(tgts), 'records'
    else :
        print 'error input for KS calculation', len(tgts), len(score)
        raise
    
    # prepare sorted score and target list
    list_score_tgt = zip(score,tgts)
    list_score_tgt = sorted(list_score_tgt,reverse=True)
    list_score_tgt=array(list_score_tgt)
    
    list_score=list_score_tgt[:,0] # score sorted
    list_tgt=list_score_tgt[:,1] # target list with score sorted
    
    # calculate cumulative count and probability
    list_tgt = array(zip(ones(len(list_tgt)),list_tgt,1-list_tgt)) # [count, tgt, non-tgt]
    cum_cnt = cumsum(list_tgt,axis=0)
    cum_prob = cum_cnt/cum_cnt[-1,:] # [pctl, tpr, fpr]
    
    # get KS and KS position
    cum_prob_diff=cum_prob[:,1]-cum_prob[:,2]
    ks = max(cum_prob_diff) # ks
    ks_pos =  squeeze(cum_prob[where(cum_prob_diff==ks),0]) # percentile pos of KS
    
    # prepare output
    threshold = list_score # score threshold
    pctl = cum_prob[:,0]#score percentiles
    tpr = cum_prob[:,1]# true positive rate (targets captured above threshold)
    fpr = cum_prob[:,2]# false positive rate (non-targets captured above threshold)
    tp_cumcnt = cum_cnt[:,1] # true positive cumulative count
    fp_cumcnt= cum_cnt[:,2] # false positive cumulative count
    


    
    # construct original lorenz curve data
    lorenz_curve = array(zip(pctl, tpr, fpr, tp_cumcnt, fp_cumcnt, threshold))
    
    # lorenz_curve sampled for capture rate (tpr) list
    capt_rate_list = list(arange(1,21)/20.)
    capt_rate_index_list = []
    for capt_rate in capt_rate_list:
        abs_tpr_diff = list(abs(tpr-capt_rate))
        i=abs_tpr_diff.index(min(abs_tpr_diff)) # get index for each capture rate
        capt_rate_index_list.append(i)
    lorenz_curve_capt_rate=list(lorenz_curve[capt_rate_index_list,:])
    lorenz_curve_capt_rate.insert(0,['score pctl','true pos rate','false pos rate','true pos cum cnt','fals pos cum cnt','score threshold'])
    
    # prepare down sample index for Lorenz curve output (this has been carefully tested to work correctly!)
    if len(list_score)>3000: # down sample to number of bins
        bin_num=1000 
    else:
        bin_num=100        
    bin_index = floor(pctl*1000)  #  assigned bin_index (0 ~ bin_num+1) to original array 
    u, ds_index = unique(bin_index, return_index=True) # find first original index with the down-sampled bin index (last bin_num+1 only has one record)
    
    # lorenz_curve down-sampled for output
    lorenz_curve_ds=list(lorenz_curve[ds_index,:]) 
    #lorenz_curve_ds=list(lorenz_curve) # no downsample
    lorenz_curve_ds.insert(0,['score pctl','true pos rate','false pos rate','true pos cum cnt','fals pos cum cnt','score threshold'])
    
    
    return [ks, ks_pos, pctl, tpr, fpr, tp_cumcnt, fp_cumcnt, threshold, lorenz_curve_ds, lorenz_curve_capt_rate]
    
