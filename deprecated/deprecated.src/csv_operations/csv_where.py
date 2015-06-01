
import csv
import gzip

def csv_where(input_file, output_file,expr):
	'''
	Keep records satisfy where expr, example of expr is below:
		expr = ['var_name','==','1'] #if number
		expr = ['var_name','==','"Y"'] #if char, add two double quotation
	Author: Jun He 8/26/2013
	'''
	

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
	field_list = infile.fieldnames
	outfile.writerow(field_list)# write header

	for row in infile:
		code='if row["'+expr[0]+'"]'+str(expr[1])+str(expr[2])+':outfile.writerow([row[key] for key in field_list])'
		#print code
		exec(code)
	
'''
input_file = '/data1/projects/CHUB0005-1/jhe/data_var_creation/data/Line_Hist_sort_by_SYSSRC_PICTS.csv.gz' 
output_file = '/data1/projects/CHUB0005-1/jhe/data_var_creation/data/Line_Hist_sort_by_SYSSRC_PICTS_where_SIU=Y.csv.gz'  
expr = ['SIU_C','==','"Y"']

csv_where(input_file, output_file,expr)
'''
