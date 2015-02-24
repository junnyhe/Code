import csv
import gzip

# change delimiter to "|"

def convert_data(infile_name,outfile_name):
    # remove space in header, and strange characters in data
    infile=gzip.open(infile_name,'rb')
    incsv=csv.DictReader(infile)
    
    
    outfile=open(outfile_name,'w')
    outcsv=csv.writer(outfile,delimiter=',')
    
    header=incsv.fieldnames
    header.remove('merge_key')
    header.remove('merge_ind')

    outcsv.writerow(header)
    
    for row in incsv:
        for var in header:
            if row[var]=="":
                row[var]="?"
        outcsv.writerow([row[var].replace('"','').replace("'","").replace(",","|").replace('\n','').replace('\r','').replace('#','apt') for var in header])
    
    outfile.close()


infile_name='/Users/junhe/Documents/Data/Model_Data_Signal_Tmx/model_data_ds_ins_imp_woe.csv.gz'
outfile_name='/Users/junhe/Documents/Results/weka/model_data_ds_ins_imp_woe.csv'

convert_data(infile_name,outfile_name)


infile_name='/Users/junhe/Documents/Data/Model_Data_Signal_Tmx/model_data_ds_oos_imp_woe.csv.gz'
outfile_name='/Users/junhe/Documents/Results/weka/model_data_ds_oos_imp_woe.csv'

convert_data(infile_name,outfile_name)

