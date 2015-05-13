################################################ def csv_set_sort ###########################################################	
import csv
import gzip

def csv_set(file_list, output_file, file_tag=()):
	'''
	This function sets/concatenates multiple CSV files.
	The files need to have the same filds, and in the same order.
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
			
	Output sorted csv file, spit out to disk
	
	Update: 2012/12/5 automatically process "gzip" file
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
		if file_name[-2:]=='gz':
			f_list.append(gzip.open(file_name,'rb'))
		else:
			f_list.append(open(file_name,'rb'))
		infile_list.append(csv.reader(f_list[i],delimiter=','))
		header_list.append(infile_list[i].next()) # get header
	
	if file_list[0][-2:]=='gz':
		fout =gzip.open(output_file+'.gz','wb')
	else:
		fout =open(output_file,'wb')
	outfile = csv.writer(fout,dialect='excel')

	# load data to list data
	data = []
	for i,infile in enumerate(infile_list):
		for row in infile:
			row.append(file_tag[i])
			data.append(row)

	# write file to disk
	header=header_list[1]
	header.append('file_tag')
	outfile.writerows([header])
	outfile.writerows(data)
	for f in f_list:
		f.close()
	fout.close()