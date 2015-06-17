import sys
import os
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
    return pymysql.connect(host='localhost', port=53306, user='junhe-ro', passwd='lohUj8B$jGn&!y1*Zb2BTsT5&BQ^mAHq', db='fraud')

connection = create_connection()

out_dir = "/fraud_model/Data/Raw_Data/targets/"
# Date to pull target data
if len(sys.argv) <=1: # if day is not specified by stdin
    year=2015
    month=3
    day=31
    nDays=1
else:
    year=int(sys.argv[1])
    month=int(sys.argv[2])
    day=int(sys.argv[3])
    nDays=int(sys.argv[4])
    
day=datetime.date(year,month,day)

for iDay in range(nDays):
    
    print "pulling targets for ",str(day)
    
    cursor = connection.cursor(pymysql.cursors.SSCursor)
    query="""
        
        Select
        
        payment_request_id,
        payer_account_id,
        payee_account_id,
        blacklisted,
        blacklist_reason,
        group_id
        
        from (
        
         -- payer with ap, direction=1, payee with group
        (Select 
        
        pr.id as payment_request_id,
        pr.payer_account_id, 
        pr.payee_account_id, 
        ap.account_id, 
        ap.value as blacklisted,
        ap2.value as blacklist_reason,
        grp.id as group_id,
        pr.direction
        from 
        
        (select 
        pr0.id,
        pr0.payer_account_id,
        pr0.payee_account_id,
        pmt.direction
        from
        wepay.payment_requests as pr0
        
        left join
        wepay.payments as pmt
        on
        pr0.id = pmt.payment_request_id
        
        where 
        pr0.create_time between unix_timestamp('"""+str(day)+"""') AND unix_timestamp('"""+str(day+datetime.timedelta(1))+"""') 
        and pr0.payer_account_id >0
        ) as pr
        
        inner join 
        wepay.account_properties as ap
        on 
        (pr.payer_account_id = ap.account_id ) -- payer with ap, direction=1, payee with group
        and ap.key="blacklisted" 
        and ap.value =1
        
        inner join 
        wepay.account_properties as ap2
        on 
        ap.account_id = ap2.account_id 
        and ap2.key="blacklist_reason" and ap2.value="Fraud"
        and pr.direction = 1 -- payer with ap, direction=1, payee with group
        
        left join
        wepay.groups as grp
        on 
        pr.payee_account_id = grp.account_id -- payer with ap, direction=1, payee with group
        )
        
        UNION
        
        -- payer with ap, direction=2, payer with group
        (Select 
        
        pr.id as payment_request_id,
        pr.payer_account_id, 
        pr.payee_account_id, 
        ap.account_id, 
        ap.value as blacklisted,
        ap2.value as blacklist_reason,
        grp.id as group_id,
        pr.direction
        from 
        
        (select 
        pr0.id,
        pr0.payer_account_id,
        pr0.payee_account_id,
        pmt.direction
        from
        wepay.payment_requests as pr0
        
        left join
        wepay.payments as pmt
        on
        pr0.id = pmt.payment_request_id
        
        where 
        pr0.create_time between unix_timestamp('"""+str(day)+"""') AND unix_timestamp('"""+str(day+datetime.timedelta(1))+"""') 
        and pr0.payer_account_id >0
        ) as pr
        
        inner join 
        wepay.account_properties as ap
        on 
        (pr.payer_account_id = ap.account_id ) -- payer with ap, direction=2, payer with group
        and ap.key="blacklisted" 
        and ap.value =1
        
        inner join 
        wepay.account_properties as ap2
        on 
        ap.account_id = ap2.account_id 
        and ap2.key="blacklist_reason" and ap2.value="Fraud"
        and pr.direction = 2  -- payer with ap, direction=2, payer with group
        
        left join
        wepay.groups as grp
        on 
        pr.payer_account_id = grp.account_id -- payer with ap, direction=2, payer with group
        )
        
        UNION
        
         -- payee with ap, direction=1, payee with group
        (Select 
        
        pr.id as payment_request_id,
        pr.payer_account_id, 
        pr.payee_account_id, 
        ap.account_id, 
        ap.value as blacklisted,
        ap2.value as blacklist_reason,
        grp.id as group_id,
        pr.direction
        from 
        
        (select 
        pr0.id,
        pr0.payer_account_id,
        pr0.payee_account_id,
        pmt.direction
        from
        wepay.payment_requests as pr0
        
        left join
        wepay.payments as pmt
        on
        pr0.id = pmt.payment_request_id
        
        where 
        pr0.create_time between unix_timestamp('"""+str(day)+"""') AND unix_timestamp('"""+str(day+datetime.timedelta(1))+"""') 
        and pr0.payer_account_id >0
        ) as pr
        
        inner join 
        wepay.account_properties as ap
        on 
        (pr.payee_account_id = ap.account_id ) -- payee with ap, direction=1, payee with group
        and ap.key="blacklisted" 
        and ap.value =1
        
        inner join 
        wepay.account_properties as ap2
        on 
        ap.account_id = ap2.account_id 
        and ap2.key="blacklist_reason" and ap2.value="Fraud"
        and pr.direction = 1 -- payee with ap, direction=1, payee with group
        
        left join
        wepay.groups as grp
        on 
        pr.payee_account_id = grp.account_id -- payee with ap, direction=1, payee with group
        )
        
        UNION
        
        -- payee with ap, direction=2, payer with group
        (Select 
        
        pr.id as payment_request_id,
        pr.payer_account_id, 
        pr.payee_account_id, 
        ap.account_id, 
        ap.value as blacklisted,
        ap2.value as blacklist_reason,
        grp.id as group_id,
        pr.direction
        from 
        
        (select 
        pr0.id,
        pr0.payer_account_id,
        pr0.payee_account_id,
        pmt.direction
        from
        wepay.payment_requests as pr0
        
        left join
        wepay.payments as pmt
        on
        pr0.id = pmt.payment_request_id
        
        where 
        pr0.create_time between unix_timestamp('"""+str(day)+"""') AND unix_timestamp('"""+str(day+datetime.timedelta(1))+"""') 
        and pr0.payer_account_id >0
        ) as pr
        
        inner join 
        wepay.account_properties as ap
        on 
        (pr.payee_account_id = ap.account_id ) -- payee with ap, direction=2, payer with group
        and ap.key="blacklisted" 
        and ap.value =1
        
        inner join 
        wepay.account_properties as ap2
        on 
        ap.account_id = ap2.account_id 
        and ap2.key="blacklist_reason" and ap2.value="Fraud"
        and pr.direction = 2  -- payee with ap, direction=2, payer with group
        
        left join
        wepay.groups as grp
        on 
        pr.payer_account_id = grp.account_id -- payee with ap, direction=2, payer with group
        )
        
        ) as fourunions
        
        group by
        payment_request_id
        
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
        'blacklist_reason',
        'group_id'
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
    
    print "scp "+out_dir+"fraud_target_"+str(day)+".csv.gz junhe@riskanalytics:/fraud_model/Data/Raw_Data/rule_results_pmt_direction/"
    os.system("scp "+out_dir+"fraud_target_"+str(day)+".csv.gz junhe@riskanalytics:/fraud_model/Data/Raw_Data/rule_results_pmt_direction/")
    
    day = day+datetime.timedelta(1)
    

connection.close()