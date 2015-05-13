import csv
import sys
import copy
import gzip

def create_merge_key(row, key_list):
	row['merge_key']=''
	for key in key_list:
		row['merge_key']=row['merge_key']+str(row[key])
	return row


def csv_key_mapping(in_file, map_file, key_list, out_file):
	"""
	This function maps (attaches) contents in map_file to in_file by lookup keys,
	map_file has no duplicates by lookup keys (will be forced to dedupped by code if not).
	"""
	import copy
	if in_file[-2:]=='gz':
		fin= gzip.open(in_file,'rb')
	else:
		fin= open(in_file,'rb')
	if map_file[-2:]=='gz':
		fmap= gzip.open(map_file,'rb')
	else:
		fmap= open(map_file,'rb')
	if out_file[-2:]=='gz':
		fout= gzip.open(out_file,'wb')
	else:
		fout= open(out_file,'wb')

	infile = csv.DictReader(fin,delimiter=',')
	mapfile = csv.DictReader(fmap,delimiter=',')
	field_list = infile.fieldnames+mapfile.fieldnames
	
	header_new=[]
	empty_row = {}
	for key in field_list:
		if not(key in empty_row):
			empty_row[key]=''
			header_new.append(key)
	header_new.append('merge_key')
	print header_new
	outfile = csv.DictWriter(fout, fieldnames=header_new,dialect='excel')
	outfile.writeheader()

	data = {}
	for row in mapfile:
		create_merge_key(row, key_list)
		data[row['merge_key']]=row

	for row in infile:
		create_merge_key(row, key_list)
		if row['merge_key'] in data:
			temp=copy.deepcopy(data[row['merge_key']])
			temp.update(row)
			outfile.writerow(temp)
		else:
			outfile.writerow(row)

if __name__== '__main__':
	sys.path.append('C:\\Users\\jhe.SDOS\\Documents\\Equifax Bustout\\code\\csv_operations')
	import csv_ops
	from csv_ops import *
	folder = 'C:\\Users\\jhe.SDOS\\Documents\\Equifax Bustout\\code\\csv_operations\\test_data\\'

	file1 = folder+'Data1.csv'
	file2 = folder+'Data2.csv'

	file1_sort_dedup = folder+'Data1_sort_dedup.csv'
	file2_sort_dedup = folder+'Data2_sort_dedup.csv'

	file_out = folder+'Data_key_map.csv'
	csv_sort_dedup(file1,file1_sort_dedup,(0,1),1)
	csv_sort_dedup(file2,file2_sort_dedup,(0,1),1)

	key_list = ['key1','key2']
	
	csv_key_mapping(file1, file2_sort_dedup, key_list, file_out)
