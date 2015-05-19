import csv
import gzip
import os
import datetime
import copy as cp
from numpy import *


#===============================================================================
# cat all rule fire review data in time range
#just need to be run once
#===============================================================================

def int_conv(n,default=0):
    try:
        return int(n)
    except:
        return default

def find_rule_list():
    
    # sample files to find rule number in date range
    print "Sample files to find rule numbers in date range"
    rule_set=set([])
    day=dayStart
    for iDay in range(nDays): 
        print "sample day:",str(day)
        input_file="/fraud_model/Data/Raw_Data/rule_fire_review_results/rule_fire_and_review_results_"+str(day)+".csv.gz"
        infile=gzip.open(input_file,'rb')
        incsv=csv.DictReader(infile)
        
        nRow=0
        for row in incsv:
            try:
                rule_set.add(int(row['rule_id']))
            except:
                continue
            nRow+=1
            if nRow>20000:
                break
        
        #increment day by one
        day = day+datetime.timedelta(1)
    
    rule_list = sorted(list(rule_set))
    
    print rule_list
    print "number of rules:",len(rule_set)
    return rule_list

def load_score_dict():
    global score_pmt_dict, score_wd_dict,header_scores
    
    # load payment model scores
    score_pmt_dict={}
    day=dayStart
    for iDay in range(nDays):
    
        print "loading payment model score for day: ",str(day)
            
        input_file="/fraud_model/Data/Raw_Data/rule_fire_review_results/model_score_payment_"+str(day)+".csv.gz"
        infile=gzip.open(input_file,'rb')
        incsv=csv.DictReader(infile)
        
        header_scores=cp.deepcopy(incsv.fieldnames)
        print header_scores
        
        row_flat = {}
        payment_request_id=''
        nRow=0
        for row in incsv:
            score_pmt_dict[row['payment_request_id']]=row
            nRow+=1
            if nRow%10000 ==0:
                print nRow,' rows are processed'
                     
        print "Totally ", nRow, " rows are processed for date:",day
        
        #increment day by one
        day = day+datetime.timedelta(1)
    
    
    # load withdrawal model scores
    score_wd_dict={}
    day=dayStart
    for iDay in range(nDays):
    
        print "loading withdrawal model score for day: ",str(day)
            
        input_file="/fraud_model/Data/Raw_Data/rule_fire_review_results/model_score_withdrawal_"+str(day)+".csv.gz"
        infile=gzip.open(input_file,'rb')
        incsv=csv.DictReader(infile)
        
        row_flat = {}
        payment_request_id=''
        nRow=0
        for row in incsv:
            score_wd_dict[row['payment_request_id']]=row
            nRow+=1
            if nRow%2000 ==0:
                print nRow,' rows are processed'
                     
        print "Totally ", nRow, " rows are processed for date:",day
        
        #increment day by one
        day = day+datetime.timedelta(1)
    
    print "N pmt is:",len(score_pmt_dict),"\nN wd is:",len(score_wd_dict)


def rollup_rules(output_file_pmt,output_file_wd):
    
    prefix='rule_'
    header_out=['payment_request_id','amount']+[prefix+str(rule_num) for rule_num in rule_list]+['target_state','target_deny'] + header_scores[1:]

    outfile_pmt=open(output_file_pmt,'w')
    outcsv_pmt=csv.writer(outfile_pmt)
    outcsv_pmt.writerow(header_out)
    
    outfile_wd=open(output_file_wd,'w')
    outcsv_wd=csv.writer(outfile_wd)
    outcsv_wd.writerow(header_out)
    
    day=dayStart
    for iDay in range(nDays):
    
        print "rolling up threatmetrix signals for day: ",str(day)
            
        input_file="/fraud_model/Data/Raw_Data/rule_fire_review_results/rule_fire_and_review_results_"+str(day)+".csv.gz"
        infile=gzip.open(input_file,'rb')
        incsv=csv.DictReader(infile)
        
        row_flat = {}
        payment_request_id=''
        nRow=0
        nID=0
        for row in incsv:
            if row['payment_request_id'] == '':
                print "id not valid: ",row['payment_request_id']
                continue
            # when reach new id, write row_flat to disk, then update id, targets for next row_flat
            if row['payment_request_id'] != payment_request_id:
                #output previous row if it's new ID and if not first time payment_request_id
                if nRow != 0:
                    # join with scores
                    if row_flat['payment_request_id'] in score_pmt_dict:
                        row_flat.update(score_pmt_dict[row_flat['payment_request_id']])
                        outcsv_pmt.writerow([row_flat.get(var,'') for var in header_out])
                    if row_flat['payment_request_id'] in score_wd_dict:
                        row_flat.update(score_wd_dict[row_flat['payment_request_id']])
                        outcsv_wd.writerow([row_flat.get(var,'') for var in header_out])
                    nID+=1
                payment_request_id = row['payment_request_id']
                #reset row_flat for next row and update new id, targets
                row_flat = {}
                row_flat['payment_request_id'] = row['payment_request_id']
                row_flat['target_state'] = row['target_state']
                row_flat['target_deny'] = row['target_deny']
                
            # no matter reach new id, always take value of key
            if row['rule_id'] != '':
                # populate rule execution in row dict
                row_flat[prefix+str(row['rule_id'])] = row['execution']
        
            nRow+=1
            if nRow%100000 ==0:
                print nRow,' rows are processed'
                
                
        #end of file output last row_flat
        if row_flat['payment_request_id'] in score_pmt_dict:
            row_flat.update(score_pmt_dict[row_flat['payment_request_id']])
            outcsv_pmt.writerow([row_flat.get(var,'') for var in header_out])
        if row_flat['payment_request_id'] in score_wd_dict:
            row_flat.update(score_wd_dict[row_flat['payment_request_id']])
            outcsv_wd.writerow([row_flat.get(var,'') for var in header_out])
        print "Totally ", nRow, " rows are processed for date:",day
        print "Totally ", nID, " payment requests are processed"
        
        #increment day by one
        day = day+datetime.timedelta(1)


    

global score_pmt_dict, score_wd_dict, rule_list, dayStart,nDays

dayStart=datetime.date(2015,4,10) #start date
nDays=5 # number of days to process

# load scores to dict
load_score_dict()

# load rule list
rule_list = find_rule_list()

# rollup and join
output_file_pmt = "/fraud_model/Data/model_rule_optimization/rule_score_pmt_20150409_20150415.csv"
output_file_wd  = "/fraud_model/Data/model_rule_optimization/rule_score_wd_20150409_20150415.csv"
rollup_rules(output_file_pmt,output_file_wd)


