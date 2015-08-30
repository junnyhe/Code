import random
import copy
import math
from numpy import *
    
def ks_roc(tgts, score):
    '''
    this version is older version, does not handle: down sampling of good, score percentile has duplicate
    '''
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
        bin_index = floor(pctl*1000)  #  assigned bin_index (0 ~ bin_num+1) to original array 
        u, ds_index = unique(bin_index, return_index=True) # find first original index with the down-sampled bin index (last bin_num+1 only has one record)
        # lorenz_curve down-sampled for output
        lorenz_curve_ds=list(lorenz_curve[ds_index,:]) 
    else:
        lorenz_curve_ds=list(lorenz_curve) # no downsample
    
    lorenz_curve_ds.insert(0,['score pctl','true pos rate','false pos rate','true pos cum cnt','fals pos cum cnt','score threshold'])
    
    
    return [ks, ks_pos, pctl, tpr, fpr, tp_cumcnt, fp_cumcnt, threshold, lorenz_curve_ds, lorenz_curve_capt_rate]
    




def ks_roc_precision(tgts, score, good_downsample_rate=1):
    '''
    this version handles both 
    *down sampling of good
    *score percentile has duplicate
    '''
    
    # sanity check
    if len(tgts) == len(score):
        print 'KS input data size', len(tgts), 'records'
    else :
        print 'error input for KS calculation', len(tgts), len(score)
        raise
    
    #===========================================================================
    # calculate perctl, tpr, fpr 
    #===========================================================================
    
    # prepare sorted score and target list
    list_score_tgt = zip(score,tgts)
    list_score_tgt = sorted(list_score_tgt,reverse=True)
    list_score_tgt = array(list_score_tgt)
    
    list_score=list_score_tgt[:,0] # score sorted
    list_tgt=list_score_tgt[:,1] # target list with score sorted
    
    # calculate cumulative count and probability
    list_tgt = array(zip(ones(len(list_tgt)),list_tgt,1-list_tgt)) # [count, tgt, non-tgt]
    cum_cnt = cumsum(list_tgt,axis=0)
    cum_prob = cum_cnt/cum_cnt[-1,:] # [pctl, tpr, fpr]
    
    
    # prepare output
    threshold = list_score # score threshold
    pctl = cum_prob[:,0]#score percentiles
    tpr = cum_prob[:,1]# true positive rate (targets captured above threshold)
    fpr = cum_prob[:,2]# false positive rate (non-targets captured above threshold)
    tp_cumcnt = cum_cnt[:,1] # true positive cumulative count
    fp_cumcnt = cum_cnt[:,2] # false positive cumulative count
    
    #===========================================================================
    # calculate refer, recall, precision with scaled population counts
    #===========================================================================
    
    # calculate scaled percentile(refer), recall, precision
    list_tgt=list_score_tgt[:,1] # target list with score sorted
    list_tgt_scaled = array(zip(ones(len(list_tgt)),list_tgt,1-list_tgt,ones(len(list_tgt)) )) # [count, tgt, non-tgt, place_holder]
    list_tgt_scaled[:,2] = list_tgt_scaled[:,2]/good_downsample_rate # scale non-tgt
    list_tgt_scaled[:,0] = list_tgt_scaled[:,1] + list_tgt_scaled[:,2] # scale counts
    
    # calculate cumulative count and probability
    cum_cnt_scaled = cumsum(list_tgt_scaled,axis=0)
    cum_prob_scaled = cum_cnt_scaled/cum_cnt_scaled[-1,:] # [pctl_scaled (refer), tpr(recall), fpr, place_holder for precision]
    cum_prob_scaled[:,3] = cum_cnt_scaled[:,1]/(cum_cnt_scaled[:,1]+cum_cnt_scaled[:,2]) # precision
    
    # prepare output for precision and recall
    Refer = cum_prob_scaled[:,0]#score percentiles
    Recall = cum_prob_scaled[:,1]# true positive rate (targets captured above threshold)
    Precision = cum_prob_scaled[:,3]# false positive rate (non-targets captured above threshold)

    
    # construct original lorenz curve data
    lorenz_curve = array(zip(pctl, tpr, fpr, tp_cumcnt, fp_cumcnt, threshold, Refer, Recall, Precision))
    
    
    # dedupe by score threshold
    lorenz_curve_deduped = []
    tmp=lorenz_curve[0]
    for i in range(1,len(lorenz_curve)):
        if lorenz_curve[i][5]<tmp[5]:
            lorenz_curve_deduped.append(tmp) #append previous row only if next row score is smaller
        tmp=lorenz_curve[i]
    lorenz_curve_deduped.append(tmp) # append last row
    lorenz_curve = lorenz_curve_deduped 
    
    
    # get KS and KS position (work on score deduped results)
    lorenz_curve=array(lorenz_curve)
    cum_prob_diff=lorenz_curve[:,1]-lorenz_curve[:,2]
    ks = max(cum_prob_diff) # ks
    ks_pos =  squeeze(cum_prob[where(cum_prob_diff==ks),0]) # percentile pos of KS
    
    
    # lorenz_curve sampled for capture rate (tpr) list
    capt_rate_list = list(arange(1,21)/20.)
    capt_rate_index_list = []
    for capt_rate in capt_rate_list:
        abs_tpr_diff = list(abs(lorenz_curve[:,1]-capt_rate))
        i=abs_tpr_diff.index(min(abs_tpr_diff)) # get index for each capture rate
        capt_rate_index_list.append(i)
    lorenz_curve_capt_rate=list(lorenz_curve[capt_rate_index_list,:])
    lorenz_curve_capt_rate.insert(0,['score pctl','true pos rate','false pos rate','true pos cum cnt','fals pos cum cnt','Score Threshold','Refer','Recall','Precision'])
    

    
    # prepare output
    
    # prepare down sample index for Lorenz curve output (this has been carefully tested to work correctly!)
    if len(lorenz_curve)>3000: # down sample to number of bins
        bin_num=1000
        bin_index = floor(lorenz_curve[:,0]*1000)  #  assigned bin_index (0 ~ bin_num+1) to deduped array 
        u, ds_index = unique(bin_index, return_index=True) # find first original index with the down-sampled bin index (last bin_num+1 only has one record)
        lorenz_curve_ds=list(lorenz_curve[ds_index,:]) 
    else:    
        lorenz_curve_ds=list(lorenz_curve) 
    

    #lorenz_curve_ds=list(lorenz_curve) # no downsample
    lorenz_curve_ds.insert(0,['score pctl','true pos rate','false pos rate','true pos cum cnt','fals pos cum cnt','Score Threshold','Refer','Recall','Precision'])
    
    
    return [ks, ks_pos, pctl, tpr, fpr, tp_cumcnt, fp_cumcnt, threshold, lorenz_curve_ds, lorenz_curve_capt_rate]
    
