import csv
import gzip
import datetime
import pymysql

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False
    
def create_connection():
    return pymysql.connect(host='prd-analydb02', port=53306, user='junhe-ro', passwd='lohUj8B$jGn&!y1*Zb2BTsT5&BQ^mAHq', db='fraud')

connection = create_connection()

out_dir = "../Data/Raw_Data/rule_results_pmt_direction/"
day=datetime.date(2015,1,1)
nDays=59

for iDay in range(nDays):
    
    print "pulling rule results and payment direction for ",str(day)
    
    cursor = connection.cursor(pymysql.cursors.SSCursor)
    query="""
        Select 
        pr.id as payment_request_id,
        fre.action,
        pmt.direction
        from 
        wepay.payment_requests pr
        left join
        fraud.fraud_rules_executions fre
        on
        pr.id = fre.pr_id
        
        left join
        wepay.payments pmt
        on
        pr.id = pmt.payment_request_id

        where pr.create_time between unix_timestamp('"""+str(day)+"""') AND unix_timestamp('"""+str(day+datetime.timedelta(1))+"""') 
        """
    #print query
    cursor.execute(query)
    
    output_file=out_dir+"fraud_rule_results_pmt_direction_"+str(day)+".csv.gz"
    outfile=gzip.open(output_file,'w')
    outcsv=csv.writer(outfile)
    
    header_out = [ 'payment_request_id',
        'action',
        'direction'
        ]
    outcsv.writerow(header_out)
    
    nRow=0
    for row in cursor:
        #print(row)
        outcsv.writerow(row)
        nRow+=1
        if nRow%100000 ==0:
            print nRow
        
    cursor.close()
    
    print nRow,"rows processed for day",str(day)
    day = day+datetime.timedelta(1)
    

connection.close()