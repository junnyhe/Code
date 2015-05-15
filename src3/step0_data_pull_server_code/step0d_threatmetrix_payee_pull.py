import csv
import gzip
import datetime
import pymysql
def create_connection():
    return pymysql.connect(host='prd-analydb02', port=53306, user='junhe-ro', passwd='lohUj8B$jGn&!y1*Zb2BTsT5&BQ^mAHq', db='fraud')

connection = create_connection()

out_dir = "../Data/Raw_Data/threatmetrix_payee/"
day=datetime.date(2015,1,1)
nDays=59

for iDay in range(nDays):
    
    print "pulling threatmetrix payee for ",str(day)
    
    cursor = connection.cursor(pymysql.cursors.SSCursor)
    query="""        
    Select
    tr_pr.payment_request_id,
    tr_pr.threatmetrix_request_time,
    tr_pr.request_id,
    tr_pr.request_type,
    trn.threatmetrix_request_id,
    trn.key,
    trn.value
      
    from
      (Select * from -- dedupe
        (Select -- join groups, threatmetrix_requests
        pr.payment_request_id,
        tr.id as threatmetrix_request_id,
        tr.request_id,
        tr.request_type,
        tr.request_time as threatmetrix_request_time
        from 
          (Select -- find pr in time range
          id  as payment_request_id,
          payee_account_id,
          create_time
          from
          wepay.payment_requests
          where
          create_time between unix_timestamp('"""+str(day)+"""') AND unix_timestamp('"""+str(day+datetime.timedelta(1))+"""') 
          ) as pr -- find pr in time range
        
        left join
        wepay.groups as grp
        on 
        pr.payee_account_id = grp.account_id
        
        left join
        log.threatmetrix_requests as tr
        on 
        grp.financial_admin_id=tr.user_id
        and tr.request_time<pr.create_time
            
        order by pr.payment_request_id,tr.request_time desc
        )  tmp -- join groups, threatmetrix_requests
       group by payment_request_id
      ) tr_pr -- dedupe
      
    left join 
    fraud.threatmetrix_request_normalized as trn
    on
    tr_pr.threatmetrix_request_id = trn.threatmetrix_request_id
    """
    print query
    cursor.execute(query)
    
    output_file=out_dir+"threatmetrix_payee_"+str(day)+".csv.gz"
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