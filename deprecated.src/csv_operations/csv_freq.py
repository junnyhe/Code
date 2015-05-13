################################################ def csv_freq ###########################################################
#! /usr/bin/env python
import csv
from numpy import *
import sys
import pickle
    
def genTable(ix,input_file):
	tbl = {}
	for line in input_file:
		cols = line.split(',')
		key = cols[ix]
		tbl[key] = tbl.get(key, 0) + 1
	return tbl


def csv_freq(input_file_name,output_file_name,col,print_flag=''):
	'''
	Enhanced freq function with SAS like output. Run freq of given column, with switch to turn on and off print.
	'''
	input_file = open(input_file_name,'r')     # Type the name of the input data file you want to run the edd on, in the quotes
	csvin = csv.reader(input_file)
	header = csvin.next()
	if col>=len(header):
		var_name= 'No_name'
	else:
		var_name = header[col]
	tbl = genTable(col,input_file)
	
	output_file = open(output_file_name, 'wb')    # Type the name which you would like to give to the EDD output file, in the quotes
	f_var = csv.writer(output_file)
	# calc stats
	tot = array(tbl.values()).sum()
	if print_flag !='off':
		print 'Proc Freq of Variable: '+var_name
		print len(tbl), 'discrete values'
		print '%s %s %s %s %s' % ("Variable","Freq","CumFreq","Percent","CumPercent")
	f_var.writerow( ['Proc Freq of Variable: '+var_name])
	f_var.writerow( [var_name,"Freq","CumFreq","Percent","CumPercent"])

	cum_count = 0;
	for key in sorted(tbl.keys()):
		pct = 100. * float(tbl[key])/float(tot)
		cum_count += tbl[key]
		cum_pct = cum_count/float(tot)*100
		if print_flag !='off':
			print '%s\t %d\t %d\t %5.2f%%\t %5.2f%%' % (key, tbl[key],cum_count, pct, cum_pct )
		f_var.writerow( [ key, tbl[key], cum_count, str(round(pct,4))+'%',str(round(cum_pct,4))+'%'])

	output_file.close()
	input_file.close()

