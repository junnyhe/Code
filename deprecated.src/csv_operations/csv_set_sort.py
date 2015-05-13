################################################ def csv_set_sort ###########################################################	
import csv
from operator import itemgetter

def csv_set_sort(file_list, output_file, sort_col_index=(0,), file_tag=()):
	'''
	This function sets/concatenates multiple CSV files, and sort the resulting file.
	The files need to have the same filds, and in the same order.
	It can sort by multiple of sort keys (as the sort function does). 
	Delimiter is ',' (can be changed in code)
	The first row is assumed to be the data header.
	Three input fields:
		file_list: input a list of file path+name to be set together
			file_list= [
			'/EQFX0001-1/users/jhe/03.DataPreparation/test/opera_testfile_201010_v2.txt',
			'/EQFX0001-1/users/jhe/03.DataPreparation/test/opera_testfile_201011_v2.txt'
			]
		output_file: output file path+name
			output_file = 'opera_testfile_201010_11_v2_sorted.csv'
		file_tag:
			file_tag = ['201010','201011']
		sort_col_index: by defaul, no entry, function will sort by the first column
			sort_col_index =(0,1,7) # sort by column
			
	Output sorted csv file, spit out to disk

	Author: Jun He 2012/11/16
	'''
	
	# set default values for file_tag 
	if file_tag ==():
		file_tag=range(len(file_list))

	f_list=[]
	infile_list=[]
	header_list=[]
	print 'set csv files:'
	for i, file_name in enumerate(file_list):
		print file_name
		f_list.append(open(file_name,'rb'))
		infile_list.append(csv.reader(f_list[i],delimiter=','))
		header_list.append(infile_list[i].next()) # get header

	fout =open(output_file,'wb')
	outfile = csv.writer(fout,dialect='excel')

	# load data to list data
	data = []
	for i,infile in enumerate(infile_list):
		for row in infile:
			row.append(file_tag[i])
			data.append(row)
		
	# sort data by key specified in sort_col_index
	data.sort(key=itemgetter(*sort_col_index))

	# write file to disk
	header=header_list[1]
	header.append('file_tag')
	outfile.writerows([header])
	outfile.writerows(data)
	for f in f_list:
		f.close()
	fout.close()

