################################################ def csv_EDD ###########################################################
#!/usr/bin/env python2.7
import csv
import os
import sys
import numpy
from numpy import *
import gzip

def csv_EDD(input_file_name,delimiter=","):
	'''
	From: EDD_revised.py
		If you get and exception at "numeric = set(reader.fieldnames)" it's because
		you are not using python 2.7.  Please ask IT to update your python version.

		Usage: pipe a csv file with headers into EDD.py
		Output: EDD of the data to stdout
		Example:
			cat someData.csv | ./EDD.py > myEDD.csv
	'''
	####### This is a better version of the original EDD code "EDD.py". 
	#######

	tmpname=input_file_name.split('.')
	tmpname[0]=tmpname[0]+'_edd'
	
	if tmpname[-1]=='gz':
		input_file = gzip.open(input_file_name,'r')
		output_file_name='.'.join(tmpname[:-1])
	else:
		input_file = open(input_file_name,'r')     # Type the name of the input data file you want to run the edd on, in the quotes
		output_file_name='.'.join(tmpname)
		
	output_file = open(output_file_name, 'wb') 
	#######

	reader = csv.DictReader(input_file,delimiter=delimiter)
	print reader.fieldnames
	
	numeric = set(reader.fieldnames)
	categoric = set()
	numStats = dict([(i, []) for i in numeric])
	catStats = {}
	blankStats = dict([(i, 0) for i in numeric])
	# Read in lines
	print >> sys.stderr, 'Reading file'
	for (ct, row) in enumerate(reader):
		if ct % 10000 == 0:
			print >> sys.stderr, '\r%i'%(ct),
		for (k, v) in row.iteritems():
			if v == '':
				blankStats[k] += 1
				continue
			if k in numeric:
				try:
					numStats[k].append(float(v))
				except ValueError:
					# This field is now categoric
					numeric.remove(k)
					categoric.add(k)
					catStats[k] = [str(i) for i in numStats[k]]
					catStats[k].append(v)
					del numStats[k]
			else:
				catStats[k].append(v)
	print >> sys.stderr, '\r%i'%(ct)
	# Calculate statistics
	print >> sys.stderr, 'Calculating statistics'
	mins = {}
	maxs = {}
	means = {}
	meds = {}
	stds = {}
	modes = {}
	for f in numeric:
		if len(numStats[f]) == 0:
			mins[f] = maxs[f] = means[f] = meds[f] = stds[f] = None
		else:
			x = array(numStats[f])
			mins[f] = x.min()
			maxs[f] = x.max()
			means[f] = mean(x)
			meds[f] = median(x)
			stds[f] = std(x)
	catVals = {}
	for f in categoric:
		vals = {}
		for v in catStats[f]:
			try:
				vals[v] += 1
			except KeyError:
				vals[v] = 1
		vals = vals.items()
		vals.sort(key = lambda x: x[1], reverse = True)
		if vals[0][1] == 1:
			catVals[f] = 'All Unique'
		else:
			catVals[f] = ['%s:%i'%(i[0], i[1]) for i in vals]
			catVals[f] = ' | '.join(catVals[f][:10])
	# Output results
	headers = ['Field Num', 'Field Name', 'Type', 'Num Blanks', 'Num Entries',
		'Num Unique', 'Min', 'Max', 'Mean', 'Median', 'Stddev',
		'Top 10 Cat Values']
	writer = csv.DictWriter(output_file, headers, lineterminator = '\n')
	writer.writeheader()
	for (ct, f) in enumerate(reader.fieldnames):
		if f in numeric:
			writer.writerow({'Field Num' : ct+1, 'Field Name' : f, 'Type' : 'Num', 
				'Num Blanks' : blankStats[f], 'Num Entries' : len(numStats[f]),
				'Num Unique' : len(set(numStats[f])), 'Min' : mins[f],
				'Max' : maxs[f], 'Mean' : means[f], 'Median' : meds[f],
				'Stddev' : stds[f]})
		else:
			writer.writerow({'Field Num' : ct+1, 'Field Name' : f, 'Type' : 'Cat',
				'Num Blanks' : blankStats[f], 'Num Entries' : len(catStats[f]),
				'Num Unique' : len(set(catStats[f])),
				'Top 10 Cat Values' : catVals[f]})

	output_file.close()
	input_file.close()
