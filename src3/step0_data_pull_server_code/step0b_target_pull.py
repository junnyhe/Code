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
    return pymysql.connect(host='proxy1', port=53310, user='junhe-ro', passwd='lohUj8B$jGn&!y1*Zb2BTsT5&BQ^mAHq', db='fraud')

connection = create_connection()

out_dir = "../Data/Raw_Data/targets/"
day=datetime.date(2014,12,1)
nDays=62

for iDay in range(nDays):
    
    print "pulling targets for ",str(day)
    
    cursor = connection.cursor(pymysql.cursors.SSCursor)
    query="""
        Select 
        
        pr.id as payment_request_id,
        pr.payer_account_id, 
        pr.payee_account_id, 
        ap.value as blacklisted, 
        ap2.value as blacklist_reason 
        
        from 
        wepay.payment_requests as pr
        
        inner join 
        wepay.account_properties as ap
        on 
        pr.payer_account_id = ap.account_id 
        and ap.key="blacklisted" 
        and ap.value =1
        
        inner join 
        wepay.account_properties as ap2
        on 
        ap.account_id = ap2.account_id 
        and ap2.key="blacklist_reason" 
        and
        pr.create_time between unix_timestamp('"""+str(day)+"""') AND unix_timestamp('"""+str(day+datetime.timedelta(1))+"""') 
        """
    #print query
    cursor.execute(query)
    
    output_file=out_dir+"fraud_target_"+str(day)+".csv.gz"
    outfile=gzip.open(output_file,'w')
    outcsv=csv.writer(outfile)
    
    header_out = [ 'payment_request_id',
        'payer_account_id',
        'payee_account_id',
        'blacklisted',
        'blacklist_reason'
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