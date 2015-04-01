import pymysql
def create_connection():
	return pymysql.connect(host='localhost', port=53306, user='junhe-ro', passwd='lohUj8B$jGn&!y1*Zb2BTsT5&BQ^mAHq', db='fraud')

connection = create_connection()
cursor = connection.cursor(pymysql.cursors.SSCursor)
""" cursor.execute(""select
            id,currency,sender_id,recipient_id,payer_account_id,payee_account_id,primary_payment_id,refunded_payment_request_id,app_account_id,amount,effective_gross,net,fee,app_fee,fee_schedule_id,fee_payer,payment_type,create_time,modify_time,finish_time,product,state,flags,version,escrow_account_id,amount_refunded,chargebacked_payment_request_id,amount_chargebacked,payment_count,refund_count,chargeback_count
            from payment_requests
            order by id desc
            limit 100"") 
"""
cursor.execute("Select * from fraud.fraud_signals where `time` between unix_timestamp('2014-07-01') AND unix_timestamp('2014-07-31') limit 100")
for r in cursor:
	print(r)
cursor.close()
connection.close()
