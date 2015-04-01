import csv
import gzip
import datetime
import pymysql
def create_connection():
	return pymysql.connect(host='prd-analydb02', port=53306, user='junhe-ro', passwd='lohUj8B$jGn&!y1*Zb2BTsT5&BQ^mAHq', db='fraud')

connection = create_connection()

out_dir = "../Data/Raw_Data/signals/"
day=datetime.date(2015,2,1)
nDays=28

for iDay in range(nDays):
	
	print "pulling signals for ",str(day)

	cursor = connection.cursor(pymysql.cursors.SSCursor)
	query="""
		Select
		pr.id as payment_request_id,
		pr.state,
		pr.create_time,
		fs.payment_request_id as fs_payment_request_id,
		fs.signal_id,
		fs.sgn_bool,
		fs.sgn_int,
		fs.sgn_float,
		fs.sgn_string
		from 
		wepay.payment_requests pr
		left join
		fraud.fraud_signals fs
		on
		pr.id = fs.payment_request_id
		where pr.create_time between unix_timestamp('"""+str(day)+"""') AND unix_timestamp('"""+str(day+datetime.timedelta(1))+"""') 
		"""
	print query
	cursor.execute(query)
	
	output_file=out_dir+"fraud_signal_"+str(day)+".csv.gz"
	outfile=gzip.open(output_file,'w')
	outcsv=csv.writer(outfile)
	
	header_out = ['payment_request_id','state','create_time','fs_payment_request_id','signal_id','sgn_bool','sgn_int','sgn_float','sgn_string']
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
