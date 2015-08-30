import csv
import gzip
import os
import datetime
import random
from numpy import *
import sys
sys.path.append("/fraud_model/Code/tools/csv_operations")
import csv_ops
from csv_ops import *
from multiprocessing import Pool

def train_validation_split(input_file,oos_frac,ins_file,oos_file):
    
    #oos_frac=0.2 #fraction used for oos
    #input_file=work_dir+"model_data_pmt.csv.gz"
    #ins_file=work_dir+"model_data_pmt_ins.csv.gz"
    #oos_file=work_dir+"model_data_pmt_oos.csv.gz"
    
    print "split to train and validation data sets"
    
    infile=gzip.open(input_file,'rb')
    incsv=csv.reader(infile)
    
    header=incsv.next()
    print header
    
    main_file_name=input_file.split('.')
    main_file_name=main_file_name[0]
    
    insfile=gzip.open(ins_file,'wb')
    inscsv=csv.writer(insfile)
    inscsv.writerow(header)
    
    oosfile=gzip.open(oos_file,'wb')
    ooscsv=csv.writer(oosfile)
    ooscsv.writerow(header)
    
    
    random.seed(1)
    nRow=0
    for row in incsv:
        if random.random()>oos_frac:
            inscsv.writerow(row)
        else:
            ooscsv.writerow(row)
            
        nRow+=1
        if nRow%100000 ==0:
            print nRow," rows are processed"
            
    print "Total, ", nRow," rows are processed;"
    
    infile.close()



def downsample_filter(input_file,output_file, downsamle_fieldname, downsample_field_equal_value, downsample_frac):
    
    print "Downsampling file:",input_file
    # get input
    infile=gzip.open(input_file,'rb')
    incsv=csv.DictReader(infile)
    header_out=incsv.fieldnames
    
    outfile=gzip.open(output_file,'w')
    outcsv=csv.writer(outfile)
    outcsv.writerow(header_out)
    
    random.seed(1)
    nRow=0
    for row in incsv:
        if not (row['target']=='1' and row['target2'] !='1'):# additional filter (exclude nonfraud blacklisted):
            if float(row[downsamle_fieldname])==float(downsample_field_equal_value): #downsample good
                if random.random() <  downsample_frac:
                    outcsv.writerow([row[key] for key in header_out])
            else: # keep bad
                outcsv.writerow([row[key] for key in header_out])
        else: # exclusion
            print row['target'], row['target2'], row['blacklist_reason']
            
        nRow+=1
        if nRow%100000 ==0:
            print nRow," rows are processed"


print len(sys.argv)

################################################################################
# Split ins and oos                                                            #
################################################################################
oos_frac=0.2 #fraction used for oos
if len(sys.argv) <=1:
    in_dir='/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt_signalonly_newest_time/'
    out_dir='/fraud_model/Data/Model_Data_Signal_Tmx_v2wd_signalonly_newest_time/'
    #in_dir=''
    #out_dir=''
elif len(sys.argv) ==3:
    in_dir=sys.argv[1]
    out_dir=sys.argv[2]
else:
    print "stdin input should be 0 or 2 vars, 0 using  pmt_data and wd_data location in code, 2 using input."
    

input_file=in_dir+"model_data_wd.csv.gz"
ins_file=out_dir+"model_data_wd_ins.csv.gz"
oos_file=out_dir+"model_data_wd_oos.csv.gz"

train_validation_split(input_file,oos_frac,ins_file,oos_file)


################################################################################
# Downsample every data set                                                    #
################################################################################
#downsample_frac=1.0001
downsample_frac=0.2

def downsample_filter_helper(arg):
    input_file=arg[0]
    output_file=arg[1]
    downsample_filter(input_file,output_file, downsamle_fieldname='target', downsample_field_equal_value='0', downsample_frac=downsample_frac)
    

input_list = [[out_dir+"model_data_wd_ins.csv.gz",out_dir+"model_data_wd_ins_ds.csv.gz"],
              [out_dir+"model_data_wd_oos.csv.gz",out_dir+"model_data_wd_oos_ds.csv.gz"],
              ]

for i in range(1,7):
    input_list.append([in_dir+"test_data_"+str(i)+"mo_wd.csv.gz",out_dir+"test_data_"+str(i)+"mo_wd_ds.csv.gz"])

pool = Pool(processes=8)
pool.map(downsample_filter_helper, input_list)


#csv_EDD(out_dir+'model_data_wd_oos_ds.csv.gz')

