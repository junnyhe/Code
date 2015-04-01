import csv
import gzip
import os
import datetime



#===============================================================================
# cat all tmx_reason_code data
#just need to be run once
#===============================================================================

work_dir="/home/junhe/fraud_model/Data/Raw_Data/tmx_reason_code/"

day=datetime.date(2014,7,5) #start date
nDays=180 # number of days to process


header_out=['tmx_request_id','tmx_reason_code']
output_file=work_dir+"tmx_reason_code_jul_dec.csv.gz"
outfile=gzip.open(output_file,'w')
outcsv=csv.writer(outfile)
outcsv.writerow(header_out)
    
for iDay in range(nDays):
    
    print "cat tmx_reason code for day: ",str(day)  
    

    
    
    input_file=work_dir+"ThreatMetrixEvents_"+str(day).replace('-','')+".csv"
    infile=open(input_file,'rb')
    incsv=csv.reader(infile)
    
    # skip two rows
    incsv.next() #empty
    incsv.next() #header
    
    nRow=0
    for row in incsv:
        outcsv.writerow([row[2],row[7]])
        nRow+=1
        

    print "Totally ", nRow, " rows are processed"
    
    
    #increment day by one
    day = day+datetime.timedelta(1)



