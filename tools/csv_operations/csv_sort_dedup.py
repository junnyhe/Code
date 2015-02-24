################################################ def csv_sort_dedup ###########################################################
import csv
import gzip
from operator import itemgetter


def csv_sort_dedup(input_file, output_file, sort_col_index=(0,),sort_col_type=[],tokeep = 'first'):
	'''
	Warning: not fully validated, be careful when using ! Use csv_sort_dedup_old for now
	
	This function sorts csv files by given column numbers and do dedupping, 
	with option to keep first or last of another column.
	It can sort by multiple of sort keys (as the sort function does).
	It allows data types of sort keys to be specified.
	Dilimter is ',' (can be changed in code)
	The first row is assumed to be the data header.
	
	Note: len(sort_col_index) should >=2, sort_col_index[0:-1] are sort keys, last column sort_col_index[-1]is used to determine first or last row to keep
	
	Three input fields:
		input_file = '/EQFX0001-1/users/jhe/03.DataPreparation/test/opera_testfile_201011_v2.txt' # input file path+name
		output_file = 'opera_testfile_201011_v2_sorted_dedup.csv' # output file path+name
		sort_col_index = (0,1,2) # sort by column 0,1,2; input is a tuple/list; by default, no entry, function will sort by the first column
		dedup will always be on the last col, in this case col2, 
		tokeep = 'first' # takes 'fisrt' or 'last'; by default, it keep the first (tokeep = 'first' or other); only when tokeep = 'last', it will keep the last
	Output sorted csv file, spit out to disk

	Update: 2012/12/5 automatically process "gzip" file
	Author: Jun He 2013/8/30
	'''
	# Sanity check
	if len(sort_col_index)<=1:
		print "len(sort_col_index) should >=2, sort_col_index[0:-1] are sort keys, last column sort_col_index[-1]is used to determine first or last row to keep"
		return
	
	if input_file[-2:]=='gz':
		fin = gzip.open(input_file,'rU')
	else:
		fin = open(input_file,'rU')
	if output_file[-2:]=='gz':
		output_file=output_file
		fout =gzip.open(output_file,'wb')
		fout_dups =gzip.open(output_file+'_drop.csv.gz','wb')
	else:
		fout =open(output_file,'wb')
		fout_dups =open(output_file+'_drop.csv','wb')
		
	infile = csv.reader(fin,delimiter=',')
	outfile = csv.writer(fout,dialect='excel')
	outfile_dups = csv.writer(fout_dups,dialect='excel')

	# get header
	header = infile.next()

	# load data to list data
	data = []
	if sort_col_type==[]: #by default sort_col_type=[], all types are 'str'
		for row in infile:
			data.append(row)
	elif len(sort_col_type) != len(sort_col_index):
		print 'length of sort_col_type != length of sort_col_index'
		return
	else:
		for row in infile:
			for i in range(len(sort_col_index)):
				if sort_col_type[i]=='int':
					try: row[sort_col_index[i]]=int(row[sort_col_index[i]])
					except: 
						print 'data incompatible with type specified'
						return
				elif sort_col_type[i]=='float':
					try: row[sort_col_index[i]]=float(row[sort_col_index[i]])
					except: 
						print 'data incompatible with type specified'
						return
				elif sort_col_type[i] !='str':
					print 'data has to be "float" "int" "str"'
					return
			data.append(row)
		
	# sort data by key specified in sort_col_index
	data.sort(key=itemgetter(*sort_col_index)) 
	
		
	# write file to disk
	#header.append('sort_key')
	outfile.writerows([header])
	outfile_dups.writerows([header])
	
	dedup_col=sort_col_index[-2]
	if tokeep !='last':
		
		print 'will keep the first of second to the last key'
		key_count ={}
		for row in data:
			if not (row[dedup_col] in key_count):
				key_count[row[dedup_col]] = 1
				outfile.writerows([row])
			else:
				key_count[row[dedup_col]] += 1
				outfile_dups.writerows([row])
	else:
		print 'will keep the last of second to the last key'
		previous_row = data[0]
		for i,row in enumerate(data):
			if row[dedup_col] !=previous_row[dedup_col]: # if key in temp_row not eq key in current row
				outfile.writerows([previous_row])
			elif i>0:
				outfile_dups.writerows([previous_row])
			previous_row = row
		outfile.writerows([previous_row])
		
	fin.close()
	fout.close()
	fout_dups.close()
	
def csv_sort_dedup_old(input_file, output_file, sort_col_index=(0,), dedup_by_col=(),tokeep = 'first'):
	'''
	Old version: data type always str, dedup_by_col should be specified
	still useable
	
	This function sorts csv files by given column numbers and do dedupping, 
	with option to keep first or last of another column.
	It can sort by multiple of sort keys (as the sort function does). 
	Dilimter is ',' (can be changed in code)
	The first row is assumed to be the data header.
	Three input fields:
		input_file = '/EQFX0001-1/users/jhe/03.DataPreparation/test/opera_testfile_201011_v2.txt' # input file path+name
		output_file = 'opera_testfile_201011_v2_sorted_dedup.csv' # output file path+name
		sort_col_index = (0,1,2) # sort by column 0,1,2; input is a tuple/list; by default, no entry, function will sort by the first column
		dedup_by_col=() # dedup by order of another column number, keep the first or the last by the order of 'dedup_by_col'; input is an integer; 
			by default, it's empty tuple, function does not dedup by order of another column (will keep the first one duplicate in file) 
		tokeep = 'first' # takes 'fisrt' or 'last'; by default, it keep the first (tokeep = 'first' or other); only when tokeep = 'last', it will keep the last
	Output sorted csv file, spit out to disk

	Update: 2012/12/5 automatically process "gzip" file
	Author: Jun He 2012/11/15
	'''
	if input_file[-2:]=='gz':
		fin = gzip.open(input_file,'rb')
	else:
		fin = open(input_file,'rb')
	if output_file[-2:]=='gz':
		output_file=output_file
		fout =gzip.open(output_file,'wb')
		fout_dups =gzip.open(output_file+'_drop.csv.gz','wb')
	else:
		fout =open(output_file,'wb')
		fout_dups =open(output_file+'_drop.csv','wb')
		
	infile = csv.reader(fin,delimiter=',')
	outfile = csv.writer(fout,dialect='excel')
	outfile_dups = csv.writer(fout_dups,dialect='excel')

	# get header
	header = infile.next()

	# load data to list data
	data = []
	for row in infile:

		sort_key='A'
		for col in sort_col_index:
			try:
				sort_key = sort_key+str(row[col])
			except:
				print row
				sort_key = sort_key
		if len(row)<len(header):
			for i in range(len(header)-len(row)):
				row.append('')
		row.insert(0,sort_key)
		data.append(row)
		
	# sort data by key specified in sort_col_index
	if dedup_by_col==() or type(dedup_by_col) !=int:
		print '\ndedup_by_col is not specified, will keep the first duplicate in file'
		data.sort(key=itemgetter(*[0])) # sort by new combined keys
	else:
		print '\ndedup_by_col is specified, will dedup by order of column'
		print 'will keep the '+ tokeep +' duplicate of col #'+str(dedup_by_col)
		data.sort(key=itemgetter(*[0,dedup_by_col]))# sort by new combined keys+ dedup col
		
	# write file to disk
	#header.append('sort_key')
	outfile.writerows([header])
	outfile_dups.writerows([header])
	
	if tokeep !='last':
		key_count ={}
		for row in data:
			if not (row[0] in key_count):
				key_count[row[0]] = 1
				row.pop(0) # remove the intermidiate sort key
				outfile.writerows([row])
			else:
				key_count[row[0]] += 1
				row.pop(0)
				outfile_dups.writerows([row])
	else:
		previous_row = data[0]
		for i,row in enumerate(data):
			if row[0] !=previous_row[0]: # if key in temp_row not eq key in current row
				previous_row.pop(0)
				outfile.writerows([previous_row])
			elif i>0:
				previous_row.pop(0)
				outfile_dups.writerows([previous_row])
			previous_row = row
		previous_row.pop(0)
		outfile.writerows([previous_row])
		
	fin.close()
	fout.close()
	fout_dups.close()
	
'''
# example code
input_file = '/data1/projects/CHUB0005-1/jhe/Tools/csv_operations/test_data/Data3_test_dedup.csv' # input file path+name
output_file = '/data1/projects/CHUB0005-1/jhe/Tools/csv_operations/test_data/Data3_test_dedup_sorted.csv' # output file path+name
output_file_de='/data1/projects/CHUB0005-1/jhe/Tools/csv_operations/test_data/Data3_test_dedup_sorted_de.csv'


csv_sort(input_file, output_file, sort_col_index=(0,1),sort_col_type=[]) # sort as string
csv_sort_dedup(input_file, output_file_de, sort_col_index=(0,1),sort_col_type=[]) # sort dedupe as string, keep first
csv_sort_dedup(input_file, output_file_de, sort_col_index=(0,1),sort_col_type=[],tokeep='last') # keep last

csv_sort(input_file, output_file, sort_col_index=(0,1),sort_col_type=['int','int']) # sort as numbers
csv_sort_dedup(input_file, output_file_de, sort_col_index=(0,1),sort_col_type=['int','int']) # sort dedupe as numbers, keep first
csv_sort_dedup(input_file, output_file_de, sort_col_index=(0,1),sort_col_type=['int','int'],tokeep = 'last') # keep last
'''


