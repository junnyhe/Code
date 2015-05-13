import random
import copy
import math
from numpy import *
    
def ks(tgts, score):

    list_score = []
    list_out = {}
    bin_num = 1000

    # sanity check
    if len(tgts) == len(score):
        print 'KS input data size', len(tgts), 'records'
    else :
        print 'error input for KS calculation', len(tgts), len(score)
        raise
     

    for ilst in range(len(tgts)):
        list_score.append([score[ilst],tgts[ilst],0]) # default bin_id = 0

    list_score = sorted(list_score,reverse=True)
    
    '''
    # break even for equal score records
    s0 = list_score[0][0]
    tmp_list_score = []
    fnl_list_score = []
    for i in range(len(list_score)):
        if s0 == list_score[i][0] and i != len(list_score)-1 :
            tmp_list_score.append(list_score[i])
        else:
            for j in range(len(tmp_list_score)):
                ji = int(random.random()*len(tmp_list_score))
                fnl_list_score.append(tmp_list_score.pop(ji))
            s0 = list_score[i][0]
            tmp_list_score = []
            tmp_list_score.append(list_score[i])
            if i == len(list_score)-1 :
                fnl_list_score.append(list_score[i])
    if len(fnl_list_score) != len(list_score):
        print 'error at breaking even', len(fnl_list_score), len(list_score)
        exit(0)
    list_score = copy.copy(fnl_list_score)
    '''

    # define bin index
    for i in range(len(list_score)):
        list_score[i][2] = math.floor((i) * bin_num / len(list_score)) + 1
    list_score=array(list_score)
    
    # calculate bin stats (total, target, nontarget for each bins)
    list_score_stats = []
    list_bin_cnt = []
    for i in range(bin_num):
        curr_bin_list = squeeze(list_score[where(list_score[:,2]==i+1),:])
        max_score = curr_bin_list[0,0] # max score
        min_score = curr_bin_list[-1,0] # min score
        mean_score = mean(curr_bin_list[:,0]) # mean score
        tgt_cnt = sum(curr_bin_list[:,1]) # total target count
        tot_cnt = len(curr_bin_list) # total record count
        nontgt_cnt = tot_cnt - tgt_cnt
        list_score_stats.append([min_score,max_score,mean_score])
        list_bin_cnt.append([tot_cnt, tgt_cnt, nontgt_cnt])
    list_score_stats = array(list_score_stats)
    list_bin_cnt = array(list_bin_cnt)
    
    cum_cnt = cumsum(list_bin_cnt,axis=0) #calculate cumsum for total, target, nontarget for the bins
    cum_prob =cum_cnt/cum_cnt[-1,:] # divide cumsum (matrix) by total (last row) for bintotal, target, nontarget
    
    # get KS and KS position
    cum_prob_diff=cum_prob[:,1]-cum_prob[:,2]
    ks = max(cum_prob_diff) # ks
    ks_pos =  squeeze(cum_prob[where(cum_prob_diff==ks),0]) # percentile pos of KS
    
    
    # prepare output
    threshold = list_score_stats[:,0] # score threshold (score max in bin)
    ceiling = list_score_stats[:,1] # score max in bin
    pctl = cum_prob[:,0]#score percentiles
    tpr = cum_prob[:,1]# true positive rate
    fpr = cum_prob[:,2]# false positive rate
    tp_cumcnt = cum_cnt[:,1] # true positive cumulative count
    fp_cumcnt= cum_cnt[:,2] # false positive cumulative count
    
    # construct lorenz curve data
    lorenz_curve = list(concatenate((cum_prob,cum_cnt,list_score_stats),axis=1) )
    lorenz_curve.insert(0,['score pctl','cum prob tgt','cum prob non-tgt','cum total bin cnt','cum tgt cnt','cum non-tgt cnt',
                           'min_score','max_score','mean_score'])
    
        
    return [ks, ks_pos, pctl, tpr, fpr, tp_cumcnt, fp_cumcnt, threshold, lorenz_curve]

