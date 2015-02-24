'''
This file contains two functions that merge two files.
csv_merge:
	The two input files have to be sorted and deduped by keys first
	(using function csv_sort_dedup, other sorting may not work properly)
csv_merge2:
	The two input files don't need to be sort and deduped by keys first, 
	but one record will be taken from each file, output file will be sorted and deduped.
'''

import csv
import sys
import copy
import gzip

def create_merge_key(row, key_list):
	row['merge_key']=''
	for key in key_list:
		row['merge_key']=row['merge_key']+str(row[key])
	return row


def csv_merge(file1, file2, key_list, file_out):
	"""
	the two input files have to be sorted and deduped by keys first
	(using function csv_sort_dedup, other sorting may not work properly)
	"""
	import copy
	if file1[-2:]=='gz':
		fin1= gzip.open(file1,'rb')
	else:
		fin1= open(file1,'rb')
	if file2[-2:]=='gz':
		fin2= gzip.open(file2,'rb')
	else:
		fin2= open(file2,'rb')
	if file_out[-2:]=='gz':
		fout= gzip.open(file_out,'wb')
	else:
		fout= open(file_out,'wb')

	infile1 = csv.DictReader(fin1,delimiter=',')
	infile2 = csv.DictReader(fin2,delimiter=',')
	field_list = infile1.fieldnames+infile2.fieldnames

	header_new=[]
	empty_row = {}
	for key in field_list:
		if not(key in empty_row):
			empty_row[key]=''
			header_new.append(key)
	#header_new.append('merge_key') # do not attach merge key and ind
	#header_new.append('merge_ind')
	outfile = csv.writer(fout)
	outfile.writerow(header_new)

	row1 = infile1.next()
	create_merge_key(row1, key_list)
	row2 = infile2.next()
	create_merge_key(row2, key_list)
	EOF1_flag=0
	EOF2_flag=0
	while EOF1_flag==0 and EOF2_flag==0:
		if row1['merge_key']<row2['merge_key'] or EOF2_flag==1:
			temp=copy.deepcopy(empty_row)
			temp.update(row1)
			temp['merge_ind']="10"
			outfile.writerow([temp[var] for var in header_new])
			try:
				row1 = infile1.next()
				create_merge_key(row1, key_list)
			except:
				EOF1_flag=1
			
		if row1['merge_key']>row2['merge_key']or EOF1_flag==1:
			temp=copy.deepcopy(empty_row)
			temp.update(row2)
			temp['merge_ind']="01"
			outfile.writerow([temp[var] for var in header_new])
			try:
				row2 = infile2.next()
				create_merge_key(row2, key_list)
			except:
				EOF2_flag=1
			
		if row1['merge_key']==row2['merge_key']:
			temp=copy.deepcopy(row2)
			temp.update(row1)
			temp['merge_ind']="11"
			outfile.writerow([temp[var] for var in header_new])
			try:
				row1 = infile1.next()
				create_merge_key(row1, key_list)
			except:
				EOF1_flag=1
			try:
				row2 = infile2.next()
				create_merge_key(row2, key_list)
			except:
				EOF2_flag=1
				
				
def csv_merge2(file1, file2, key_list_1,  key_list_2, file_out, how='outer'):
	"""
	Note: enhanced version of 'csv_merge()', changes: 
	  1. Allow key names to be different from two files,
	  2. Allow joining mode to be specified in 'how'
	Input data requirement:
	  the two input files have to be sorted and deduped by keys first
	  (using function csv_sort_dedup, other sorting may not work properly)
	Inputs:
	  file1, file2 are input left and right files
	  key_list_1,  key_list_2 are sorted keys from left and right table, conditions will be key_list_1[0] = key_list_2[0] ....
	  how = 'outer','inner', 'left' (outer by default)
	"""
	#sanity check
	if how not in ['outer','inner', 'left']:
		print "how must be 'outer','inner', 'left'"
		return
	if len(key_list_1) != len(key_list_2):
		print "number of keys must be the same from two files"
		return
	
	import copy
	if file1[-2:]=='gz':
		fin1= gzip.open(file1,'rb')
	else:
		fin1= open(file1,'rb')
	if file2[-2:]=='gz':
		fin2= gzip.open(file2,'rb')
	else:
		fin2= open(file2,'rb')
	if file_out[-2:]=='gz':
		fout= gzip.open(file_out,'wb')
	else:
		fout= open(file_out,'wb')

	infile1 = csv.DictReader(fin1,delimiter=',')
	infile2 = csv.DictReader(fin2,delimiter=',')
	field_list = infile1.fieldnames+infile2.fieldnames

	header_new=[]
	empty_row = {}
	for key in field_list:
		if not(key in empty_row):
			empty_row[key]=''
			header_new.append(key)
	#header_new.append('merge_key') # do not attach merge key and ind
	#header_new.append('merge_ind')
	outfile = csv.writer(fout)
	outfile.writerow(header_new)

	row1 = infile1.next()
	create_merge_key(row1, key_list_1)
	row2 = infile2.next()
	create_merge_key(row2, key_list_2)
	EOF1_flag=0
	EOF2_flag=0
	while EOF1_flag==0 and EOF2_flag==0:
		if row1['merge_key']<row2['merge_key'] or EOF2_flag==1:
			temp=copy.deepcopy(empty_row)
			temp.update(row1)
			temp['merge_ind']="10"
			try:
				row1 = infile1.next()
				create_merge_key(row1, key_list_1)
			except:
				EOF1_flag=1
			
		if row1['merge_key']>row2['merge_key']or EOF1_flag==1:
			temp=copy.deepcopy(empty_row)
			temp.update(row2)
			temp['merge_ind']="01"
			try:
				row2 = infile2.next()
				create_merge_key(row2, key_list_2)
			except:
				EOF2_flag=1
			
		if row1['merge_key']==row2['merge_key']:
			temp=copy.deepcopy(row2)
			temp.update(row1)
			temp['merge_ind']="11"
			try:
				row1 = infile1.next()
				create_merge_key(row1, key_list_1)
			except:
				EOF1_flag=1
			try:
				row2 = infile2.next()
				create_merge_key(row2, key_list_2)
			except:
				EOF2_flag=1
		
		if how=='outer':
			outfile.writerow([temp[var] for var in header_new])
		elif how=='inner':
			if temp['merge_ind']=="11":
				outfile.writerow([temp[var] for var in header_new])
		elif how=='left':
			if temp['merge_ind']=="11" or temp['merge_ind']=="10":
				outfile.writerow([temp[var] for var in header_new])
		else:
			print "how must be 'left','inner','outer'"

				
				
def csv_merge2_deprecated(file1, file2, key_list, file_out):
	"""
	! not recommended to use ! 
	! be careful when use this version, the files may need to be sorted/deduped to merge correctly,otherwise may produce error !
	the two input files don't need to be sorted and deduped by keys first
	output file will be deduped
	if there are duplicates, the last records in both file have priority
	and the last record in the first file has the most priority to update resulting record
	If the input files are sorted and deduped, output will be the same as csv_merge
	"""
	import copy
	if file1[-2:]=='gz':
		fin1= gzip.open(file1,'rb')
	else:
		fin1= open(file1,'rb')
	if file2[-2:]=='gz':
		fin2= gzip.open(file2,'rb')
	else:
		fin2= open(file2,'rb')
	if file_out[-2:]=='gz':
		fout= gzip.open(file_out,'wb')
	else:
		fout= open(file_out,'wb')

	infile1 = csv.DictReader(fin1,delimiter=',')
	infile2 = csv.DictReader(fin2,delimiter=',')
	field_list = infile1.fieldnames+infile2.fieldnames

	header_new=[]
	empty_row = {}
	for key in field_list:
		if not(key in empty_row):
			empty_row[key]=''
			header_new.append(key)
	header_new.append('merge_key')
	header_new.append('merge_ind')
	outfile = csv.DictWriter(fout, fieldnames=header_new,dialect='excel')
	outfile.writeheader()

	data = {}
	for row in infile2:
		create_merge_key(row, key_list)
		row['merge_ind']="01"
		data[row['merge_key']]=row

	for row in infile1:
		create_merge_key(row, key_list)
		if row['merge_key'] in data:
			row['merge_ind']="11"
			data[row['merge_key']].update(row)
		else:
			row['merge_ind']="10"
			data[row['merge_key']]=row
			
	for row_key in sorted(data.keys()):
		outfile.writerow(data[row_key])
		


def csv_leftjoin(file1, file2, key_list, file_out, tokeep='all'):
	"""
	the two input files don't need to be sort and deduped by keys first
	file2 is small, keys should be unique, will be hashed to memory
	file1 is big, keys can have duplicates, result file
	left join will attach record in file2 to file1,
	result file will have the same keys/record as file1.
	tokeep='all': keep everything in file1; tokeep='11': only keep common keys.
	"""
	import copy
	if file1[-2:]=='gz':
		fin1= gzip.open(file1,'rb')
	else:
		fin1= open(file1,'rU')
	if file2[-2:]=='gz':
		fin2= gzip.open(file2,'rb')
	else:
		fin2= open(file2,'rU')
	if file_out[-2:]=='gz':
		fout= gzip.open(file_out,'wb')
	else:
		fout= open(file_out,'wb')

	infile1 = csv.DictReader(fin1,delimiter=',')
	infile2 = csv.DictReader(fin2,delimiter=',')
	field_list = infile1.fieldnames+infile2.fieldnames

	header_new=[]
	empty_row = {}
	for key in field_list:
		if not(key in empty_row):
			empty_row[key]=''
			header_new.append(key)
	#header_new.append('merge_key') # not to show merge key
	header_new.append('merge_ind')
	outfile = csv.writer(fout)
	outfile.writerow(header_new)

	hashTable2 = {}
	for row in infile2:
		create_merge_key(row, key_list)
		#print row['merge_key']
		hashTable2[row['merge_key']]=row

	for row in infile1:
		create_merge_key(row, key_list)
		try: # remove leading zeros
			row['merge_key']=str(int(row['merge_key']))
		except:
			row['merge_key']=row['merge_key']
		
		if row['merge_key'] in hashTable2:
			row['merge_ind']="11"
			row.update(hashTable2[row['merge_key']])
		else:
			row['merge_ind']="10"
		
		if tokeep=="11":
			if row['merge_ind']=="11":
				outfile.writerow([row[var] for var in header_new])
		else: #tokeep="all"
			outfile.writerow([row[var] for var in header_new])


				
if __name__== '__main__':
	sys.path.append('C:\\Users\\jhe.SDOS\\Documents\\Equifax Bustout\\code\\csv_operations')
	import csv_ops
	from csv_ops import *
	folder = 'C:\\Users\\jhe.SDOS\\Documents\\Equifax Bustout\\code\\csv_operations\\test_data\\'

	file1 = folder+'Data1.csv'
	file2 = folder+'Data2.csv'

	file1_sort_dedup = folder+'Data1_sort_dedup.csv'
	file2_sort_dedup = folder+'Data2_sort_dedup.csv'

	file_out = folder+'Data_merged.csv'
	file_out2 = folder+'Data_merged2.csv'
	csv_sort_dedup(file1,file1_sort_dedup,(0,1),1)
	csv_sort_dedup(file2,file2_sort_dedup,(0,1),1)

	key_list = ['key1','key2']
	
	csv_merge(file1_sort_dedup, file2_sort_dedup, key_list, file_out)
	csv_merge2(file1_sort_dedup, file2_sort_dedup, key_list, file_out2)
