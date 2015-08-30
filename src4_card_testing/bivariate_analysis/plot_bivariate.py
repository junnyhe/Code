import csv
import pandas as pd
import random
import numpy as np
from collections import defaultdict
from sklearn.ensemble import RandomForestClassifier
import sys
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


def plot_bivariate(input_file,var_list_file,signal_type_file,signal_descp_file,target_name,out_dir):
    '''
    #input data file name with path (has to be gz for now)
    input_file="/fraud_model/Data/card_testing/model_data_pmt_ins_ds_imp_woe_vc.csv.gz"
    
    #list of variable names to plot bivariate (can be subset of full variable list)
    var_list_file="/fraud_model/Code/src4_card_testing/support_files/model_var_list_name_analytics_gib.csv"
    
    #signal data types
    signal_type_file="sigType.csv"
    e.g.
    (no header, 4 types)
    signal_1,int
    signal_2,cat
    signal_4,bool
    signal_8,float
    ...

    #signal descriptions
    signal_descp_file="sigDescp.csv"
    e.g.:
    (no header)
    signal_1,Signal_Payer_Email_Verified
    signal_2,Signal_Avs_Matches
    signal_4,Signal_Credit_Card_Name_Match_Username
    ...
    
    #target name in the data set
    target_name='target_ct'
    
    #location to output figures
    out_dir="/fraud_model/Results/card_testing/bivariate_analysis/"
    '''
    
    print "loading input file: " + input_file
    
    df = pd.read_csv(open(input_file), sep = ',',compression='gzip')
    
    print "loading variable list: " + str(var_list_file)
    bivarPlot = []
    fin = csv.reader(open(var_list_file,'rU'))
    for row in fin:
        bivarPlot.append(row[0])
    
    
    sigType = {}
    fin = csv.reader(open(signal_type_file,'rU'))
    for row in fin:
        sigType[row[0]] = row[1]
    #print "Types:\n",sigType
    
    sigDescp = {}
    fin = csv.reader(open(signal_descp_file,'rU'))
    for row in fin:
        sigDescp[row[0]] = row[1]
    print "Descriptions:\n",sigDescp
    
    
    plotPoints = 10
    scale = range(0,100, int(100.0/plotPoints))
    
    #bivarPlot = list(set(chooseList).difference(set(other)))
    for v in bivarPlot:
        print sigDescp[v]
        x = []
        y = []
        z = []
        label = []
        tmp = df[[target_name,v]]
        if sigType[v] == "bool" or sigType[v] == "cat" or (sigType[v] == "int" and len(tmp[v].unique())<=20):
            tmp[v] = tmp[v].fillna("unknown")
            a = list(tmp[v].unique())
            for i in range(len(a)):
                x.append(i*2)
                label.append(str(a[i]))
                y.append(np.sum(tmp[v] == a[i]))
                z.append(tmp[tmp[v] == a[i]][target_name].mean())
                
            if sigType[v] == "int":
                zp = zip(label,y,z)
                zp = sorted(zp,key = lambda l:l[0])
                label,y,z = zip(*zp)
    
            if len(x)>20:
                zp = zip(label,y,z)
                zp = sorted(zp,key = lambda l:l[1],reverse=True)
                zp = zp[0:20]
                if sigType[v] == "int":
                    zp = sorted(zp,key = lambda l:l[0])
                label,y,z = zip(*zp)
                x = x[0:20]
            
                
        elif (sigType[v] == "int" and len(tmp[v].unique())>20) or sigType[v] == "float":
            bound = []
            for i in scale:
                bound.append(tmp[v].quantile(i/100.0))
            bound.append(tmp[v].max()+1)
            bound = sorted(list(set(bound)))
            for i in range(len(bound)-1):
                x.append(i*2)
                y.append(np.sum((tmp[v] >= bound[i]) & (tmp[v] < bound[i+1])))
                label.append("["+str(bound[i])+","+str(bound[i+1])+")")
                z.append(tmp[(tmp[v] >= bound[i]) & (tmp[v] < bound[i+1])][target_name].mean())
    
            if np.sum(tmp[v].isnull())>0:
                x.append((i+1)*2)
                y.append(np.sum(tmp[v].isnull()))
                label.append("NaN")
                z.append(tmp[tmp[v].isnull()][target_name].mean())
    
        print x,y,z
        fig, ax1 = plt.subplots()
        plt.title(v+":"+sigDescp[v])
        ax2 = ax1.twinx()
        ax1.bar(x, y, 1/1.5, color="blue",align='center')
        ax2.plot(x,z,linestyle='-',marker='o',color="red")
        fig.autofmt_xdate()
        plt.xticks(x,label)
        ax1.set_xlabel("Value Range")
        ax1.set_ylabel("Counts",color="blue")
        ax2.set_ylabel("Bad Rate",color="red")
    
        fig.savefig(out_dir+v+":"+sigDescp[v]+".png")
        plt.close(fig)
    

if __name__=="__main__":
    input_file="/fraud_model/Data/card_testing/model_data_pmt_ins_ds_imp_woe_vc.csv.gz"
    var_list_file="/fraud_model/Code/src4_card_testing/support_files/model_var_list_name_analytics_gib.csv"
    signal_type_file="sigType.csv"
    signal_descp_file="sigDescp.csv"
    target_name='target_ct'
    out_dir="/fraud_model/Results/card_testing/bivariate_analysis/"
    
    plot_bivariate(input_file,var_list_file,signal_type_file,signal_descp_file,target_name,out_dir)
    
    
    