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

		Usage: pipe a csv file with header into EDD.py
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
					catStats[k] = [str(val) for val in numStats[k]] # transfer values from numStats
					catStats[k].append(v) # append new val to cat value list
					del numStats[k]
			else:
				catStats[k].append(v)
	print >> sys.stderr, '\r%i'%(ct)
	# Calculate statistics
	print >> sys.stderr, 'Calculating statistics'
	means = {}
	meds = {}
	stds = {}
	mins = {}
	maxs = {}
	p1 = {}
	p5 = {}
	p10 = {}
	p25 = {}
	p50 = {}
	p75 = {}
	p90 ={}
	p95 = {}
	p99 = {}
	uniqStats = {}
	numVals = {}
	for f in numeric:
		print "calculating stats for numeric var:", f
		# stats
		if len(numStats[f]) == 0:
			mins[f] = maxs[f] = means[f] = meds[f] = stds[f] = None
			p1[f]=p5[f]=p10[f]=p25[f]=p50[f]=p75[f]=p90[f]=p95[f]=p99[f]=None
		else:
			x = array(numStats[f])
			means[f] = mean(x)
			meds[f] = median(x)
			stds[f] = std(x)
			mins[f] = x.min()
			maxs[f] = x.max()
			p1[f]  = percentile(x, 1)
			p5[f]  = percentile(x, 5)
			p10[f] = percentile(x, 10)
			p25[f] = percentile(x, 25)
			p50[f] = meds[f]
			p75[f] = percentile(x, 75)
			p90[f] = percentile(x, 90)
			p95[f] = percentile(x, 95)
			p99[f] = percentile(x, 99)
		# uniq cnts
		uniqStats[f] = len(set(numStats[f]))
		# val cnts
		if uniqStats[f]<=20:
			vals = {}
			for v in numStats[f]:
				try:
					vals[v] += 1
				except KeyError:
					vals[v] = 1
			vals = vals.items()
			vals.sort(key = lambda x: x[1], reverse = True)
			numVals[f] = ['%s:%i'%(str(i[0]).rstrip('0').rstrip('.'), i[1]) for i in vals]
			numVals[f] = ' | '.join(numVals[f][:20])
		else:
			numVals[f]=''
			
	catVals = {}
	for f in categoric:
		print "calculating stats for categorical var:", f
		# uniq cnts
		uniqStats[f] = len(set(catStats[f]))
		# val cnts
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
			catVals[f] = ' | '.join(catVals[f][:20])
			
		

			
	# Output results
	header = ['Field Num', 'Field Name', 'Type', 'Num Blanks', 'Num Entries',
		'Num Unique', 'Top 20 Cat Values',
		'Mean', 'Median', 'Stddev',
		'Min', 'P1', 'P5', 'P10', 'P25', 'P50', 'P75', 'P90', 'P95', 'P99','Max'
		]
	#writer = csv.DictWriter(output_file, header, lineterminator = '\n')
	#writer.writeheader()
	writer = csv.writer(output_file)
	writer.writerow(header)
	for (ct, f) in enumerate(reader.fieldnames):
		
		if f in numeric:
			print "write results for numeric var:", ct, f
			result_row={'Field Num' : ct+1, 'Field Name' : f, 'Type' : 'Num', 
				'Num Blanks' : blankStats[f], 'Num Entries' : len(numStats[f]),
				'Num Unique' : uniqStats[f], # do not output number of unique for numerics to speed up
				'Top 20 Cat Values' : numVals[f],
				'Mean' : means[f], 'Median' : meds[f],'Stddev' : stds[f],
				'Min' : mins[f],'Max' : maxs[f],
				'P1' : p1.get(f,''), 'P5' : p5.get(f,''), 'P10' : p10.get(f,''), 'P25' : p25.get(f,''), 'P50' : p50.get(f,''), 'P75' : p75.get(f,''), 'P90' : p90.get(f,''), 'P95' : p95.get(f,''), 'P99' : p99.get(f,'')
				}
			writer.writerow([result_row.get(key,'') for key in header])
		else:
			print "write results for categorical var:", ct, f
			result_row={'Field Num' : ct+1, 'Field Name' : f, 'Type' : 'Cat',
				'Num Blanks' : blankStats[f], 'Num Entries' : len(catStats[f]),
				'Num Unique' : uniqStats[f],
				'Top 20 Cat Values' : catVals[f]}
			writer.writerow([result_row.get(key,'') for key in header])

	output_file.close()
	input_file.close()
