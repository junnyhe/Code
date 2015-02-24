################################################ def csv_freq ###########################################################
#! /usr/bin/env python
import csv
import gzip
from numpy import *
import sys
import pickle
    

def csv_freq3(input_file_name,var_list,indir,outdir,print_flag=''):
	'''
	Powerful version: Input is a list variable names. 
	Enhanced freq function with SAS like output. Run freq of given column, with switch to turn on and off print.
	Author: Jun He 2012/12/5
	'''
	if input_file_name[-2:] =='gz':
		input_file = gzip.open(indir+input_file_name,'r')
	else:
		input_file = open(indir+input_file_name,'r')     # Type the name of the input data file you want to run the edd on, in the quotes
	csvin = csv.DictReader(input_file)
	
	# use dict of dict to store the counts, initialize
	tbl = {}
	for var_name in var_list:
		tbl[var_name] = {}
	
	# accumulate the counts
	for row in csvin:
		for var_name in var_list:
			try:
				var_value = int(row[var_name])
			except:
				var_value = row[var_name]
			tbl[var_name][var_value] = tbl[var_name].get(var_value, 0) + 1
	

	for i,var_name in enumerate(var_list):
		output_file = open(outdir+'freq-'+var_name+'-'+input_file_name, 'wb')    # Type the name which you would like to give to the EDD output file, in the quotes
		f_var=csv.writer(output_file)
		
		# calc stats
		tot = array(tbl[var_name].values()).sum()
		if print_flag !='off':
			print 'Proc Freq of Variable: '+var_name
			print len(tbl[var_name]), 'discrete values'
			print '%s %s %s %s %s' % ("Variable","Freq","CumFreq","Percent","CumPercent")
		f_var.writerow( ['Proc Freq of Variable: '+var_name])
		f_var.writerow( [var_name,"Freq","CumFreq","Percent","CumPercent"])

		cum_count = 0;
		for key in sorted(tbl[var_name].keys()):
			pct = 100. * float(tbl[var_name][key])/float(tot)
			cum_count += tbl[var_name][key]
			cum_pct = cum_count/float(tot)*100
			if print_flag !='off':
				print '%s\t %d\t %d\t %5.2f%%\t %5.2f%%' % (key, tbl[var_name][key],cum_count, pct, cum_pct )
			f_var.writerow( [ key, tbl[var_name][key], cum_count, str(round(pct,4))+'%',str(round(cum_pct,4))+'%'])


