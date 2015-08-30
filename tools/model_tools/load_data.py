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
    nRow=0
    for row in inscsv:
        try:
            row_float = [float(row[var]) for var in var_list+[target_name]]
            data.append(row_float)
        except:
            print "Warning: Row contains none numeric values, skipping ............"
        nRow+=1
        if nRow%10000 ==0:
            print nRow," rows loaded"
    
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
    nRow=0
    for row in inscsv:
        try:
            row_float = [float(row[var]) for var in full_var_list]
            data.append(row_float)
        except:
            print "Warning: Row contains none numeric values, skipping ............"
        nRow+=1
        if nRow%10000 ==0:
            print nRow," rows loaded"
    
    data=np.array(data)
    X=data[:,:-2]
    y=data[:,-2]
    key=data[:,-1]
    del data
    return X,y,key

def load_data_with_key_tag(input_file, var_list_filename, target_name, key_name, tag_name):
    # load data to X (dependent data), y (target)
    # also loads key/id
    # also loads another tag, similar to target
    varlist_file=open(var_list_filename,'rU')
    varlist_csv=csv.reader(varlist_file)
    var_list=[]
    for row in varlist_csv:
        var_list.append(row[0])
    
    
    full_var_list = var_list+[target_name,key_name, tag_name] # last two rows for target and key/id
    
    insfile=gzip.open(input_file,'rb')
    inscsv=csv.DictReader(insfile)
    data=[]
    nRow=0
    for row in inscsv:
        try:
            row_float = [float(row[var]) for var in full_var_list]
            data.append(row_float)
        except:
            print "Warning: Row contains none numeric values, skipping ............"
        nRow+=1
        if nRow%10000 ==0:
            print nRow," rows loaded"
    
    data=np.array(data)
    X=data[:,:-3]
    y=data[:,-3]
    key=data[:,-2]
    tag=data[:,-1]
    del data
    return X,y,key,tag


def load_data_fast(input_file, var_list_filename, target_name):
    # load data to X (dependent data), y (target)
    varlist_file=open(var_list_filename,'rU')
    varlist_csv=csv.reader(varlist_file)
    var_list=[]
    for row in varlist_csv:
        var_list.append(row[0])
    
    full_var_list = var_list+[target_name]
    
    print full_var_list

    insfile=gzip.open(input_file,'rb')
    inscsv=csv.reader(insfile)
    header = inscsv.next()

    full_var_list_index = []
    for var in full_var_list:
        try:
            full_var_list_index.append(header.index(var))
        except:
            print var,"not present in data"
    
    data=[]
    nRow=0
    for row in inscsv:
            
        try:
            data.append([float(row[i]) for i in full_var_list_index])
        except:
            print "Warning: Row contains none numeric values, skipping ............"
            for i,index in enumerate(full_var_list_index): # for test exceptions
                try:
                    float(row[index])
                except:
                    print full_var_list[i],row[index]
            
        nRow+=1
        if nRow%10000 ==0:
            print nRow," rows loaded"
    data=np.array(data)
    X=data[:,:-1]
    y=data[:,-1]
    del data
    return X,y


def load_data_with_key_fast(input_file, var_list_filename, target_name, key_name):
    # load data to X (dependent data), y (target)
    # also loads key/id
    varlist_file=open(var_list_filename,'rU')
    varlist_csv=csv.reader(varlist_file)
    
    var_list=[]
    for row in varlist_csv:
        var_list.append(row[0])
    
    
    full_var_list = var_list+[target_name,key_name] # last two rows for target and key/id
    
    insfile=gzip.open(input_file,'rb')
    inscsv=csv.reader(insfile)
    header = inscsv.next()
    for var in var_list:
        if var not in header:
            print var, "is not in header"
    
    full_var_list_index = []
    for var in full_var_list:
        full_var_list_index.append(header.index(var))

    
    data=[]
    nRow=0
    for row in inscsv:
        try:
            #data.append([row[i] for i in full_var_list_index])
            data.append([float(row[i]) for i in full_var_list_index])
            
        except:
            print "Warning: Row contains none numeric values, skipping ............"
            print row
        nRow+=1
        if nRow%10000 ==0:
            print nRow," rows loaded"
    
    data=np.array(data)
    X=data[:,:-2]
    y=data[:,-2]
    key=data[:,-1]
    del data
    return X,y,key



def load_data_with_key_tag_fast(input_file, var_list_filename, target_name, key_name, tag_name):
    # load data to X (dependent data), y (target)
    # also loads key/id
    # also loads another tag, similar to target
    varlist_file=open(var_list_filename,'rU')
    varlist_csv=csv.reader(varlist_file)
    
    var_list=[]
    for row in varlist_csv:
        var_list.append(row[0])
    
    
    full_var_list = var_list+[target_name,key_name, tag_name] # last two rows for target and key/id
    
    insfile=gzip.open(input_file,'rb')
    inscsv=csv.reader(insfile)
    header = inscsv.next()
    for var in var_list:
        if var not in header:
            print var, "is not in header"
    
    full_var_list_index = []
    for var in full_var_list:
        full_var_list_index.append(header.index(var))

    
    data=[]
    nRow=0
    for row in inscsv:
        try:
            #data.append([row[i] for i in full_var_list_index])
            data.append([float(row[i]) for i in full_var_list_index])
            
        except:
            print "Warning: Row contains none numeric values, skipping ............"
            print row
        nRow+=1
        if nRow%10000 ==0:
            print nRow," rows loaded"
    
    data=np.array(data)
    X=data[:,:-3]
    y=data[:,-3]
    key=data[:,-2]
    tag=data[:,-1]
    del data
    return X,y,key,tag



#===============================================================================
# Below are load int version of load_data_fast and load_data_with_key_tag_fast
#===============================================================================


def load_data_fast_int(input_file, var_list_filename, target_name):
    # load data to X (dependent data), y (target)
    varlist_file=open(var_list_filename,'rU')
    varlist_csv=csv.reader(varlist_file)
    var_list=[]
    for row in varlist_csv:
        var_list.append(row[0])
    
    full_var_list = var_list+[target_name]

    
    insfile=gzip.open(input_file,'rb')
    inscsv=csv.reader(insfile)
    header = inscsv.next()
    
    full_var_list_index = []
    for var in full_var_list:
        full_var_list_index.append(header.index(var))
    
    data=[]
    nRow=0
    for row in inscsv:
        try:
            #data.append([row[i] for i in full_var_list_index])
            data.append([int(row[i]) for i in full_var_list_index])
            
        except:
            print "Warning: Row contains none numeric values, skipping ............"
        nRow+=1
        if nRow%5000 ==0:
            print nRow," rows loaded"
    
    data=np.array(data)
    
    X=data[:,:-1]
    y=data[:,-1]
    del data
    return X,y


def load_data_with_key_tag_fast_int(input_file, var_list_filename, target_name, key_name, tag_name):
    # load data to X (dependent data), y (target)
    # also loads key/id
    # also loads another tag, similar to target
    varlist_file=open(var_list_filename,'rU')
    varlist_csv=csv.reader(varlist_file)
    var_list=[]
    for row in varlist_csv:
        var_list.append(row[0])
    
    
    full_var_list = var_list+[target_name,key_name, tag_name] # last two rows for target and key/id
    
    insfile=gzip.open(input_file,'rb')
    inscsv=csv.reader(insfile)
    header = inscsv.next()
    
    full_var_list_index = []
    for var in full_var_list:
        full_var_list_index.append(header.index(var))

    
    data=[]
    nRow=0
    for row in inscsv:
        try:
            #data.append([row[i] for i in full_var_list_index])
            data.append([int(row[i]) for i in full_var_list_index])
            
        except:
            print "Warning: Row contains none numeric values, skipping ............"
        nRow+=1
        if nRow%5000 ==0:
            print nRow," rows loaded"
    
    data=np.array(data)
    X=data[:,:-3]
    y=data[:,-3]
    key=data[:,-2]
    tag=data[:,-1]
    del data
    return X,y,key,tag
    