import csv
import gzip
import os
import datetime
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

def find_rule_list(dayStart,nDays):
    
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
            if nRow>10000:
                break
        
        #increment day by one
        day = day+datetime.timedelta(1)
    
    rule_list = sorted(list(rule_set))
    
    print rule_list
    print "number of rules:",len(rule_set)
    return rule_list



def rollup_rules(dayStart,nDays,output_file):
    prefix='rule_'
    header_out=['payment_request_id']+[prefix+str(rule_num) for rule_num in rule_list]+['target_state','target_deny']

    outfile=gzip.open(output_file,'w')
    outcsv=csv.writer(outfile)
    outcsv.writerow(header_out)
    
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
                    outcsv.writerow([row_flat.get(var,'') for var in header_out])
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
        outcsv.writerow([row_flat.get(var,'') for var in header_out])
        print "Totally ", nRow, " rows are processed for date:",day
        print "Totally ", nID, " payment requests are processed"
        
        #increment day by one
        day = day+datetime.timedelta(1)


    



dayStart=datetime.date(2014,12,1) #start date
nDays=31 # number of days to process

rule_list = find_rule_list(dayStart,nDays)

output_file = "/fraud_model/Data/rule_optimization/rule_fire_and_review_results_all.csv.gz"
rollup_rules(dayStart,nDays,output_file)


