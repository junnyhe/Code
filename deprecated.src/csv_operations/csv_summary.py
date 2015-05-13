import csv
import gzip
'''
These functions run SAS proc summary style summations.
A few variant are provided.
'''
def conv_impt(x):
	try:
		x= float(x)
	except:
		x=0
	return x
		
		
def csv_summary(input_file,output_file,key_list,var_list):
	'''
	This function runs SAS proc summary style summation for a list of keys and a list of varibles.
	Sum each variable for each unique combination of the keys specified in key_list.
	Author: Jun He 11/29/2012
	'''
	if input_file[-2:]=='gz':
		fin = gzip.open(input_file,'rb')
	else:
		fin = open(input_file,'rb')
	fout =open(output_file,'wb')

	infile = csv.DictReader(fin,delimiter=',')
	outfile = csv.writer(fout,dialect='excel')
	header = key_list + var_list + ['cnt']

	outfile.writerow(header)

	var_sum ={}
	var_cnt ={}
	key_dict ={}
	for row in infile:
		temp_key = ''.join([str(row[key])+"|" for key in key_list])
		if temp_key not in key_dict:
			key_dict[temp_key] = [str(row[key]) for key in key_list]
			var_sum[temp_key] = []
			for var in var_list:
				var_sum[temp_key].append(0)
			var_cnt[temp_key] = 0
		for i,var in enumerate(var_list):
				var_sum[temp_key][i] = var_sum[temp_key][i] + conv_impt(row[var])
		var_cnt[temp_key] = var_cnt[temp_key] + 1
		
	for key in sorted(var_sum.keys()):
		row = list(key_dict[key])
		row = row + var_sum[key]
		row.append(var_cnt[key])
		outfile.writerow(row)
		

def csv_summary1(input_file,output_file,key_list,var):
	'''
	This function runs SAS proc summary style summation for a list of keys and one varible.
	Sum the variable for each unique combination of the keys specified in key_list.
	This is an intermediate version in my development.
	Author: Jun He 11/29/2012
	'''
	fin = open(input_file,'rb')
	fout =open(output_file,'wb')

	infile = csv.DictReader(fin,delimiter=',')
	outfile = csv.writer(fout,dialect='excel')
	header = key_list + ['sum_'+var] + ['cnt']

	outfile.writerow(header)

	var_sum ={}
	var_cnt ={}
	key_dict ={}
	for row in infile:
		temp_key = ''.join([str(row[key])+"|" for key in key_list])
		if temp_key not in key_dict:
			key_dict[temp_key] = [str(row[key]) for key in key_list]
		var_sum[temp_key] = var_sum.get(temp_key,0) + conv_impt(row[var])
		var_cnt[temp_key] = var_cnt.get(temp_key,0) + 1
		
	for key in sorted(var_sum.keys()):
		row = list(key_dict[key])
		row.append(var_sum[key])
		row.append(var_cnt[key])
		outfile.writerow(row)
		

'''
Examples:

input_file = '/EQFX0001-1/project/02.DataCollection/02.01.RawData/Opera_CH_201010.txt'# input CSV flat file: properly sorted by consumer, trade line, month, and dedupped
output_file = 'Opera_CH_201010_consumer_sum_co.csv' # output file2: aggregated over trade line, left with consumer level monthly (consumer, month)flat file 

key ='cnid'
var = 'chrgoff'
key_list = ['cnid','drpt']
var_list = ['chrgoff','chgoff amt','bal']

csv_sum(input_file,output_file,key,var)
csv_summary1(input_file,output_file,key_list,var)
csv_summary(input_file,'file_201010_11_sum_co_ind_5.csv',key_list,var_list)

'''