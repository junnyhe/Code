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

out_dir = "../Data/Raw_Data/threatmetrix_payer/"
day=datetime.date(2014,7,1)
nDays=123

for iDay in range(nDays):
    
    print "pulling threatmetrix payer for ",str(day)
    
    cursor = connection.cursor(pymysql.cursors.SSCursor)
    query="""
        Select 
        pr_tr.payment_request_id,
        pr_tr.threatmetrix_request_time,
        pr_tr.request_id,
        pr_tr.request_type,
        trn.threatmetrix_request_id,
        trn.key,
        trn.value
        from
        (Select * from( -- dedupe
        Select 
        pr.id as payment_request_id,
        pr.create_time,
        tr.id as threatmetrix_request_id,
        tr.request_id,
        tr.request_type,
        tr.payment_request_id as tr_payment_request_id,
        tr.request_time as threatmetrix_request_time
        from 
        wepay.payment_requests as pr
        
        left join
        log.threatmetrix_requests as tr
        on 
        pr.id = tr.payment_request_id
        
        where
        pr.create_time between unix_timestamp('"""+str(day)+"""') AND unix_timestamp('"""+str(day+datetime.timedelta(1))+"""') 
        
        order by tr.payment_request_id, 
        tr.request_time desc
        ) tmp group by payment_request_id -- dedupe
        ) as pr_tr
        
        left join
        fraud.threatmetrix_request_normalized as trn
        on
        pr_tr.threatmetrix_request_id = trn.threatmetrix_request_id
        """
    print query
    cursor.execute(query)
    
    output_file=out_dir+"threatmetrix_payer_"+str(day)+".csv.gz"
    outfile=gzip.open(output_file,'w')
    outcsv=csv.writer(outfile)
    
    header_out = [ 'payment_request_id', 
        'threatmetrix_request_time',
        'request_id',
        'request_type',
        'threatmetrix_request_id',
        'key',
        'value']
    outcsv.writerow(header_out)
    
    nRow=0
    for row in cursor:
        #print(row)
        row=list(row)
        if row[4] !=None: # cleaning change of line in threatmetrix data
            if '\n' in row[6]:
                tmp=row[6].replace('\n',' ')
                row[6]=tmp
                print "Cleaning return in text:"
        
        outcsv.writerow(row)
        nRow+=1
        if nRow%100000 ==0:
            print nRow
        
    cursor.close()
    
    print nRow,"rows processed for day",str(day)
    day = day+datetime.timedelta(1)
    

connection.close()