'''
Keep the variables specified in variable keep_list
Author: Jun He
'''
import csv
import gzip

def csv_keep(input_file, output_file,keep_list):
	if input_file[-2:]=='gz':
		fin = gzip.open(input_file,'rb')
	else:
		fin = open(input_file,'rb')
	if output_file[-2:]=='gz':
		fout = gzip.open(output_file,'wb')
	else:
		fout =open(output_file,'wb')

	infile = csv.DictReader(fin,delimiter=',')
	outfile = csv.writer(fout,dialect='excel')
	outfile.writerow(keep_list)# write header

	for row in infile:
		outfile.writerow([row[key] for key in keep_list])
	
'''
input_file = 'Opera_CH_201010_consumer.csv' # input CSV flat file: properly sorted by consumer, trade line, month, and dedupped
output_file = 'Opera_CH_201010_consumer_keep.csv' # output file2: aggregated over trade line, left with consumer level monthly (consumer, month)flat file 

keep_list = ['cnid','drpt', 'sum_chrgoff', 'max_chrgoff']

csv_keep(input_file, output_file,keep_list)
'''