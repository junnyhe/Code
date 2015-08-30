import csv
import gzip

result_dir='/fraud_model/Results/card_testing/pmt_gib/'
f1 = gzip.open("/fraud_model/Data/card_testing/test_data_1mo_pmt_ds_imp_woe_vc.csv.gz", "rb")
csv1 = csv.DictReader(f1)

f2 = open(result_dir+"score_pmt_gib_1mo.csv", "rb")
csv2 = csv.DictReader(f2)


fout=open(result_dir+'test_1mo_sliced_w_score_rmEli_1tran_addLen.csv',"w")
outcsv=csv.writer(fout)
header_out=['firstName',
            'lastName',
            'email',
            'first_name_gib_score',
            'last_name_gib_score',
            'user_name_gib_score',
            'first_name_gib_name_score',
            'last_name_gib_name_score',
            'user_name_gib_name_score',
            'y',
            'score']

outcsv.writerow(header_out)
for row in csv1:
    row.update(csv2.next())
    outcsv.writerow([row[var] for var in header_out])
fout.close()
