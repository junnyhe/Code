import csv
import gzip
import numpy as np

def load_data(input_file, var_list_filename, target_name):
    # load data to X (dependent data), y (target)
    varlist_file=open(var_list_filename,'rU')
    varlist_csv=csv.reader(varlist_file)
    var_list=[]
    for row in varlist_csv:
        var_list.append(row[0])
    
    
    insfile=gzip.open(input_file,'rb')
    inscsv=csv.DictReader(insfile)
    data=[]
    for row in inscsv:
        try:
            row_float = [float(row[var]) for var in var_list+[target_name]]
            data.append(row_float)
        except:
            print "Warning: Row contains none numeric values, skipping ............"
    
    data=np.array(data)
    X=data[:,:-1]
    y=data[:,-1]
    del data
    return X,y

def load_data_with_key(input_file, var_list_filename, target_name, key_name):
    # load data to X (dependent data), y (target)
    # also loads key/id
    varlist_file=open(var_list_filename,'rU')
    varlist_csv=csv.reader(varlist_file)
    var_list=[]
    for row in varlist_csv:
        var_list.append(row[0])
    
    
    full_var_list = var_list+[target_name,key_name] # last two rows for target and key/id
    
    insfile=gzip.open(input_file,'rb')
    inscsv=csv.DictReader(insfile)
    data=[]
    for row in inscsv:
        try:
            row_float = [float(row[var]) for var in full_var_list]
            data.append(row_float)
        except:
            print "Warning: Row contains none numeric values, skipping ............"
    
    data=np.array(data)
    X=data[:,:-2]
    y=data[:,-2]
    key=data[:,-1]
    del data
    return X,y,key
    