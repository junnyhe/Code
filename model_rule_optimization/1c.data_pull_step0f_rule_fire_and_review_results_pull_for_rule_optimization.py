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

out_dir = "../Data/Raw_Data/rule_fire_review_results/"
day=datetime.date(2015,4,9)
nDays=6

for iDay in range(nDays):
    
    print "pulling rule fire and review results for ",str(day)
    
    cursor = connection.cursor(pymysql.cursors.SSCursor)
    query="""
        Select
        pr.id as payment_request_id,
        pr.state,
        pr.create_time,
        pr.state in (6,7) as target_state,
        rev.deny_code,
        not isnull(rev.deny_code) as target_deny,
        fre.action,
        fred.rule_id,
        fred.execution
        
        from 
        wepay.payment_requests pr
        
        left join
        wepay.reviews rev
        on
        pr.id = rev.payment_request_id
        
        left join
        fraud.fraud_rules_executions fre
        on
        pr.id = fre.pr_id
        
        left join
        fraud.fraud_rules_execution_details fred
        on
        fred.id = fre.id

        where pr.create_time between unix_timestamp('"""+str(day)+"""') AND unix_timestamp('"""+str(day+datetime.timedelta(1))+"""') 
        order by payment_request_id
        """
    #print query
    cursor.execute(query)
    
    output_file=out_dir+"rule_fire_and_review_results_"+str(day)+".csv.gz"
    outfile=gzip.open(output_file,'w')
    outcsv=csv.writer(outfile)
    
    header_out = [ 'payment_request_id',
                  'state',
                  'create_time',
                  'target_state',
                  'deny_code',
                  'target_deny',
                  'action',
                  'rule_id',
                  'execution'
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