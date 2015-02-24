################################################ def csv_sort ###########################################################
import csv
import gzip
from operator import itemgetter

def csv_sort(input_file, output_file, sort_col_index=(0,),sort_col_type=[]):
	'''
	This function sorts CSV files with given column numbers.
	It can sort by multiple of sort keys (as the sort function does).
	It allows data types of sort keys to be specified.
	The first row is assumed to be the data header.
	Four input fields:
		input_file = '/EQFX0001-1/users/jhe/03.DataPreparation/test/opera_testfile_201011_v2.txt'; input file path+name
		output_file = 'opera_testfile_201011_v2_sorted.csv'; output file path+name
		sort_col_index = (0,1,2); sort by column 0,1,2; # input is a tuple/list; by defaul, no entry, function will sort by the first column
		
		sort_col_type= 'str', 'int', or 'float'; 
		by default sort_col_type=[], is 'str'
	
	Output sorted csv file
	
	Update: 2013/8/27 can specify type for each variable
	Update: 2012/12/5 automatically process "gzip" file
	Author: Jun He 2012/11/15
	'''
	if input_file[-2:]=='gz':
		fin = gzip.open(input_file,'rU')
	else:
		fin = open(input_file,'rU')
	if output_file[-2:]=='gz':
		fout =gzip.open(output_file,'wb')
	else:
		fout =open(output_file,'wb')
	infile = csv.reader(fin,delimiter=',')
	outfile = csv.writer(fout,dialect='excel')

	# get header
	header = infile.next()
	cnt=0
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
			cnt+=1
			if cnt%100:
				print row
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
	outfile.writerows([header])
	outfile.writerows(data)
	fin.close()
	fout.close()

'''
example:
input_file='/data1/projects/CHUB0005-1/jhe/Tools/csv_operations/test_data/Data1.csv'
output_file='/data1/projects/CHUB0005-1/jhe/Tools/csv_operations/test_data/sort_test_out.csv'
sort_col_index=(0,)

csv_sort(input_file, output_file, sort_col_index=(0,2))

csv_sort(input_file, output_file, sort_col_index=(0,2),sort_col_type=['int','str'])
'''