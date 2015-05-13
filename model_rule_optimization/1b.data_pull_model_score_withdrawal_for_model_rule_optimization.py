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

#create a ssh tunnel: ssh -L 53306:prd-analydb02:53306 jumprep

connection = create_connection()

out_dir = "../Data/Raw_Data/rule_fire_review_results/"
day=datetime.date(2015,4,10)
nDays=5

for iDay in range(nDays):
    
    print "pulling withdrawal model scores for ",str(day)
    
    cursor = connection.cursor(pymysql.cursors.SSCursor)
    query="""
        Select
        pr.id as payment_request_id,
        pr.amount,
        -- pr.create_time,
        fs.sgn_float as score,
        pmt.direction,
        ap2.value as blacklist_reason,
        case 
        when fs.sgn_float>=0.95 then "95"
        when fs.sgn_float>=0.9 and fs.sgn_float<0.95 then "90"
        when fs.sgn_float>=0.85 and fs.sgn_float<0.9 then "85"
        when fs.sgn_float>=0.8 and fs.sgn_float<0.85 then "80"
        when fs.sgn_float>=0.75 and fs.sgn_float<0.8 then "75"
        when fs.sgn_float>=0.7 and fs.sgn_float<0.75 then "70"
        when fs.sgn_float>=0.65 and fs.sgn_float<0.7 then "65"
        when fs.sgn_float>=0.6 and fs.sgn_float<0.65 then "60"
        when fs.sgn_float>=0.55 and fs.sgn_float<0.6 then "55"
        when fs.sgn_float>=0.5 and fs.sgn_float<0.55 then "50"
        when fs.sgn_float>=0.45 and fs.sgn_float<0.5 then "45"
        when fs.sgn_float>=0.4 and fs.sgn_float<0.45 then "40"
        when fs.sgn_float>=0.35 and fs.sgn_float<0.4 then "35"
        when fs.sgn_float>=0.3 and fs.sgn_float<0.35 then "30"
        when fs.sgn_float>=0.25 and fs.sgn_float<0.3 then "25"
        when fs.sgn_float>=0.2 and fs.sgn_float<0.25 then "20"
        when fs.sgn_float>=0.15 and fs.sgn_float<0.2 then "15"
        when fs.sgn_float>=0.1 and fs.sgn_float<0.15 then "10"
        when fs.sgn_float>=0.05 and fs.sgn_float<0.1 then "5"
        when fs.sgn_float>=0 and fs.sgn_float<0.05 then "0" 
        end as score_band,
        
        case when fs.sgn_float>=0.95 then 1 else 0 end as score_ge_95,
        case when fs.sgn_float>=0.9 then 1 else 0 end as score_ge_90,
        case when fs.sgn_float>=0.85 then 1 else 0 end as score_ge_85,
        case when fs.sgn_float>=0.8 then 1 else 0 end as score_ge_80,
        case when fs.sgn_float>=0.75 then 1 else 0 end as score_ge_75,
        case when fs.sgn_float>=0.7 then 1 else 0 end as score_ge_70,
        case when fs.sgn_float>=0.65 then 1 else 0 end as score_ge_65,
        case when fs.sgn_float>=0.6 then 1 else 0 end as score_ge_60,
        case when fs.sgn_float>=0.55 then 1 else 0 end as score_ge_55,
        case when fs.sgn_float>=0.5 then 1 else 0 end as score_ge_50,
        case when fs.sgn_float>=0.45 then 1 else 0 end as score_ge_45,
        case when fs.sgn_float>=0.4 then 1 else 0 end as score_ge_40,
        case when fs.sgn_float>=0.35 then 1 else 0 end as score_ge_35,
        case when fs.sgn_float>=0.3 then 1 else 0 end as score_ge_30,
        case when fs.sgn_float>=0.25 then 1 else 0 end as score_ge_25,
        case when fs.sgn_float>=0.2 then 1 else 0 end as score_ge_20,
        case when fs.sgn_float>=0.15 then 1 else 0 end as score_ge_15,
        case when fs.sgn_float>=0.1 then 1 else 0 end as score_ge_10,
        case when fs.sgn_float>=0.05 then 1 else 0 end as score_ge_5,
        case when fs.sgn_float>=0 then 1 else 0 end as score_ge_0
        
        from
        wepay.payment_requests pr
        
        left join
        wepay.payments pmt
        on
        pr.id = pmt.payment_request_id
        
        left join
        fraud.fraud_signals fs
        on
        pr.id = fs.payment_request_id
        
        left join 
        wepay.account_properties as ap
        on 
        pr.payer_account_id = ap.account_id 
        and ap.key="blacklisted" 
        and ap.value =1
        
        left join 
        wepay.account_properties as ap2
        on 
        ap.account_id = ap2.account_id 
        and ap2.key="blacklist_reason" 

        where pr.create_time between unix_timestamp('"""+str(day)+"""') AND unix_timestamp('"""+str(day+datetime.timedelta(1))+"""') 
        and fs.signal_id=634 -- 633 score_pmt, 634 score_wd
        order by payment_request_id
        """
    #print query
    cursor.execute(query)
    
    output_file=out_dir+"model_score_withdrawal_"+str(day)+".csv.gz"
    outfile=gzip.open(output_file,'w')
    outcsv=csv.writer(outfile)
    
    header_out = [ 'payment_request_id',
                'amount',
                'score',
                'direction',
                'blacklist_reason',
                'score_band',
                'score_ge_95',
                'score_ge_90',
                'score_ge_85',
                'score_ge_80',
                'score_ge_75',
                'score_ge_70',
                'score_ge_65',
                'score_ge_60',
                'score_ge_55',
                'score_ge_50',
                'score_ge_45',
                'score_ge_40',
                'score_ge_35',
                'score_ge_30',
                'score_ge_25',
                'score_ge_20',
                'score_ge_15',
                'score_ge_10',
                'score_ge_5',
                'score_ge_0'
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