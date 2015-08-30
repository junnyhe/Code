################################################ def csv_WOE_cat ###########################################################
#! /usr/bin/env python
import csv
import gzip
from numpy import *
import cPickle as pickle
import json

    
def risk_table(work_dir, input_file, woe_var_list_file, target, smooth_num=0, target_value='1'):
    '''
    Function to compute risk_table, which will be assigned to original data
    Input:
    work_dir='/Users/junhe/Documents/Data/' # everything should/will be in this folder
    input_file='model_data_ds_ins_imp.csv.gz' # var list file
    woe_var_list_file=dir+'woe_var_list.csv' # var list file for woe
    (var list file format: one row one var name, no header, no coma, no quotes)
    target: name of the target field (target take the value of 0 and 1);
    smooth_num: number of record to smooth the log odds with average target rate, by default =0; think about 200~5000
    target_value: value when target variable is target, by default ='1'; if char type make target_value='"Y"'
    
    Output:
    risk_table.csv to folder:work_dir,
    which will be parsed by function woe_assign()
    Author: Jun He 2012/8/29; edited 2014/11/26
    '''

    print "\nCreating Risk Table ..."
    
    woe_var_file=open(woe_var_list_file,'rU')
    woe_var_csv=csv.reader(woe_var_file)
    woe_var_list=[]
    for row in woe_var_csv:
        woe_var_list.append(row[0])
    
    print woe_var_list
        
    if input_file[-2:] =='gz':
        fin = gzip.open(work_dir+input_file,'rb')
    else:
        fin = open(work_dir+input_file,'rb')
    infile = csv.DictReader(fin)
    
    
    fout = open(work_dir+'risk_table.csv','w')
    outfile = csv.writer(fout)
    
    # use dict of dict to store the counts, initialize
    table_cnt={}
    for var_name in woe_var_list:
        table_cnt[var_name]={}
        table_cnt[var_name]['target_cnt']={}
        table_cnt[var_name]['total']={}
    
    # accumulate the counts
    total_record=0
    total_target=0
    for row in infile:
        for var_name in woe_var_list:
            var_value = row[var_name]
            table_cnt[var_name]['total'][var_value] = table_cnt[var_name]['total'].get(var_value, 0) + 1
            if row[target]==target_value:
                table_cnt[var_name]['target_cnt'][var_value] = table_cnt[var_name]['target_cnt'].get(var_value, 0) + 1
        
        total_record+=1
        if row[target]==target_value:
            total_target+=1
    overall_target_rate=total_target/float(total_record)
    overall_log_odds= log(overall_target_rate/(1-overall_target_rate))
    
    #reorganize risk table in risk_table_csv and risk_table_dict
    risk_table_csv=[]
    risk_table_dict={}
    eps = 0.0000000000000001# to prevent divide by zero
    for var_name in woe_var_list:
        risk_table_dict[var_name]={}
        for var_value in table_cnt[var_name]['total']:
            
            # prepare risk_table_csv
            total        = table_cnt[var_name]['total'].get(var_value, 0)
            target_cnt   = table_cnt[var_name]['target_cnt'].get(var_value, 0)
            target_rate  = target_cnt/float(total)
            target_rate_sm= (target_cnt+overall_target_rate*smooth_num)/float(total+smooth_num)
            log_odds     =log((target_rate+eps)/(1-target_rate+eps))
            log_odds_sm  =log((target_rate_sm+eps)/(1-target_rate_sm+eps))
            #print var_name, var_value, total, target_cnt,target_rate,target_rate_sm,log_odds,log_odds_sm
            risk_table_csv.append( [ var_name, var_value, total, target_cnt,target_rate,target_rate_sm,log_odds,log_odds_sm ] )
            
            # prepare risk_table_dict
            risk_table_dict[var_name][var_value]={}
            #risk_table_dict[var_name][var_value]['total'] = total
            #risk_table_dict[var_name][var_value]['target_cnt'] = target_cnt
            #risk_table_dict[var_name][var_value]['target_rate'] = target_rate
            #risk_table_dict[var_name][var_value]['target_rate_sm'] = target_rate_sm
            #risk_table_dict[var_name][var_value]['log_odds'] = log_odds
            risk_table_dict[var_name][var_value]['log_odds_sm'] = log_odds_sm
        
        # prepare default for risk table csv
        risk_table_csv.append( [ var_name, 'default', total_record, total_target,overall_target_rate,overall_target_rate,overall_log_odds,overall_log_odds ] )
        
        # prepare default for risk table dict
        var_value='default'
        risk_table_dict[var_name][var_value]={}
        #risk_table_dict[var_name][var_value]['total'] = total_record
        #risk_table_dict[var_name][var_value]['target_cnt'] = total_target
        #risk_table_dict[var_name][var_value]['target_rate'] = overall_target_rate
        #risk_table_dict[var_name][var_value]['target_rate_sm'] = overall_target_rate
        #risk_table_dict[var_name][var_value]['log_odds'] = overall_log_odds
        risk_table_dict[var_name][var_value]['log_odds_sm'] = overall_log_odds
    
    #output risk table csv
    header=['var_name', 'var_value', 'total', 'target_cnt','target_rate','target_rate_sm','log_odds','log_odds_sm']
    outfile.writerow(header)
    outfile.writerows(risk_table_csv)
    
    #output risk table dict as pickle/json
    pickle.dump([risk_table_dict,woe_var_list],open(work_dir+"risk_table.p",'wb')) 
    #json.dump(risk_table_dict,open(work_dir+"risk_table.json",'wb'),encoding="ISO-8859-1") 
    
    print "Risk table, 'risk_table.csv' and 'risk_table.p' created at work_dir:", work_dir

    
def woe_assign(work_dir, input_file, output_file):
    '''
    Function uses output file from risk_table() in csv file and assign woe to input data
    
    Input:
    work_dir='/Users/junhe/Documents/Data/' # everything should/will be in this folder
    input_file='model_data_ds_ins_imp.csv.gz' # input data in which WOE will be assigned to var list
    load risk table from csv file: risk_table.csv  located in folder:work_dir,
    output_file='model_data_ds_ins_imp_woe.csv.gz' # output data with new WOE variables, starting with 'lo_'
    woe_var_list_file='woe_var_list.csv' # var list file for woe
    (var list file format: one row one var name, no header, no coma, no quotes)
    
    Output:
    output_file, new woe variables are appended to input file, start with prefix "lo_"
    
    Author: Jun He 2012/8/30
    '''
    
    print "\nAssigning WOE values to data:", input_file, "..."
    
    # input 
    if input_file[-2:] =='gz':
        fin = gzip.open(work_dir+input_file,'rU')
    else:
        fin = open(work_dir+input_file,'rU')
    infile = csv.DictReader(fin,delimiter=',')
    
    # output
    if output_file[-2:]=='gz':
        fout = gzip.open(work_dir+output_file,'wb')
    else:
        fout =open(work_dir+output_file,'wb')
    outfile = csv.writer(fout)
    
    
    # load risk table to dict of dict
    frs = open(work_dir+'risk_table.csv','rb') # reset file pointer
    rsfile = csv.DictReader(frs,delimiter=',')
    table={} # initialize dict of dict for risk table
    woe_var_list=[]
    for row in rsfile:
        if row['var_name'] not in table:
            woe_var_list.append(row['var_name']) # setup woe_var_list as var appeared in the risk table csv file
            table[row['var_name']]={}
        table[row['var_name']][row['var_value']]=row
        
    
    # get var list in risk table, and construct output field list
    field_list = infile.fieldnames
    field_list = field_list+['lo_'+var for var in woe_var_list]
    
    # check is all woe_var_list in field list
    for var in woe_var_list:
        if var not in field_list:
            print var, "in risk_table is not present in the data, new variable lo_"+var,"provides no prediction" 
    
    # assign woe to origianl data
    outfile.writerow(field_list)
    for row in infile:
        for var_name in woe_var_list:
            try:
                row['lo_'+var_name]= table[var_name][row[var_name]]['log_odds_sm']
            except:
                row['lo_'+var_name]= table[var_name]['default']['log_odds_sm']
                #print 'Warning: new value of the variable: ',var_name,'=',row[var_name],' is not found in the risk table. Default log_odds of overall population is assigned.'
        outfile.writerow([row[key] for key in field_list])

    print "Done assigning WOE "


   
def woe_assign_pickle(work_dir, input_file, output_file):
    '''
    Function uses output file from risk_table() in pickle file and assign woe to input data
    
    Input:
    work_dir='/Users/junhe/Documents/Data/' # everything should/will be in this folder
    input_file='model_data_ds_ins_imp.csv.gz' # input data in which WOE will be assigned to var list
    load risk table from pickle file: risk_table.p  located in folder:work_dir,
    output_file='model_data_ds_ins_imp_woe.csv.gz' # output data with new WOE variables, starting with 'lo_'
    !! no need for woe_var_list_file='woe_var_list.csv' 
    (var list file format: one row one var name, no header, no coma, no quotes)
    
    Output:
    
    output_file, new woe variables are appended to input file, start with prefix "lo_"
    
    Author: Jun He 2014/12/22
    '''
    
    print "\nAssigning WOE values to data:", input_file, "..."
    
    #input file
    if input_file[-2:] =='gz':
        fin = gzip.open(work_dir+input_file,'rb')
    else:
        fin = open(work_dir+input_file,'rb')
    infile = csv.DictReader(fin,delimiter=',')
    
    #input file
    if output_file[-2:]=='gz':
        fout = gzip.open(work_dir+output_file,'w')
    else:
        fout =open(work_dir+output_file,'w')
    outfile = csv.writer(fout)
    
    # load risk table from pickle
    table,woe_var_list = pickle.load(open(work_dir+'risk_table.p','rb')) # load both risk_table and woe_var_list(used to preserve order of vars)
    #table = json.load(open(work_dir+'risk_table.json','rb'),encoding="ISO-8859-1") # character may have problem converting back !
    
    # define new header
    field_list = infile.fieldnames
    field_list = field_list+['lo_'+var for var in woe_var_list]
    
    # assign woe to origianl data
    outfile.writerow(field_list)
    for row in infile:
        for var_name in woe_var_list:
            try:
                row['lo_'+var_name]= table[var_name][row[var_name]]['log_odds_sm']
            except:
                row['lo_'+var_name]= table[var_name]['default']['log_odds_sm']
                #print 'Warning: new value of the variable: ',var_name,'=',row[var_name],' is not found in the risk table. Default log_odds of overall population is assigned.'
        outfile.writerow([row[key] for key in field_list])

    print "Done assigning WOE "
    
''' usage examples:
work_dir='/Users/junhe/Documents/Data/'

input_file='model_data_ds_ins_imp.csv.gz'
woe_var_list_file='woe_var_list.csv'
risk_table(work_dir, input_file, woe_var_list_file, target='target', smooth_num=500, target_value='1')
risk_table(work_dir, input_file, woe_var_list_file, target='target', smooth_num=0, target_value='"Y"')

input_file='model_data_ds_ins_imp.csv.gz'
output_file='model_data_ds_ins_imp_woe.csv.gz'
woe_var_list_file='woe_var_list.csv'
woe_assign(work_dir, input_file, output_file)
'''