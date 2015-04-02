import gzip
import csv

filename1="/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v3wd_newest_time/4cmp_model_data_wd_oos_ds_rcind_fc_imp_woe.csv.gz"
file1=gzip.open(filename1,'rb')
filecsv1 = csv.DictReader(file1)

filename2="/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v3wd_newest_time/model_data_wd_oos_ds_rcind_fc_imp_woe.csv.gz"
file2=gzip.open(filename2,'rb')
filecsv2 = csv.DictReader(file2)
header = filecsv1.fieldnames

for row1 in filecsv1:
    row2 = filecsv2.next()
    for key in header:
        if row1[key] != row2[key]:
            print key,":",row1[key], row2[key]

print "If no output, it means two files are identical !"    

    
    