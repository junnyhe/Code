import pandas as pd
import random
import numpy as np
from collections import defaultdict
from sklearn.ensemble import RandomForestClassifier
import sys
import matplotlib.pyplot as plt

"""
signal = pd.read_csv(open('freshbooks_signal.csv'), sep = ',')
tag = pd.read_csv(open('freshbooks_tag.csv'),sep = ',')
df = signal.join(tag,on = 'payment_request_id' , how = 'left', lsuffix = '_signal', rsuffix = '_tag', sort = False)

df = pd.merge(tag,signal,left_on = 'payment_request_id', right_on = 'payment_request_id', how = 'right')
df['target']=df['blacklisted'].fillna(0)

a = df.describe(include="all").transpose()

population = dict(a['count']>1075)

chooseList = []
for var in population:
    if population[var]==True:
        chooseList.append(var)


df = df[chooseList]

other = ["target","payment_request_id","payer_account_id","payee_account_id","app_account_id","blacklisted","blacklist_reason","group_id","direction_x","create_time_x","state","create_time_y","fs_payment_request_id","direction_y"]


df1=df[df['direction_y']==1]
df2=df[df['direction_y']==2]
"""

python /fraud_model/Code/tools/model_tools/plot_bivariate.py

print "loading input file: " + sys.argv[1]

df = pd.read_csv(open(sys.argv[1]), sep = ',')

print "loading variable list: " + str(sys.argv[2])
bivarPlot = []
fin = open(sys.argv[2])
for l in fin:
    bivarPlot.append(l.rstrip())

# target = sys.argv[3]

sigType = {}
fin = open("sigType.csv")
for l in fin:
    ln = l.rstrip().split(',')
    sigType[ln[0]] = ln[1]

print sigType


sigName = {}
fin = open("sigName.csv")
for l in fin:
    ln = l.rstrip().split(',')
    sigName[ln[0]] = ln[1]

print sigName


plotPoints = 10
scale = range(0,100, int(100.0/plotPoints))

#bivarPlot = list(set(chooseList).difference(set(other)))
for v in bivarPlot:
    print sigName[v]
    x = []
    y = []
    z = []
    label = []
    tmp = df1[['target',v]]
    if sigType[v] == "bool" or sigType[v] == "cat" or (sigType[v] == "int" and len(tmp[v].unique())<=20):
        tmp[v] = tmp[v].fillna("unknown")
        a = list(tmp[v].unique())
        for i in range(len(a)):
            x.append(i*2)
            label.append(str(a[i]))
            y.append(np.sum(tmp[v] == a[i]))
            z.append(tmp[tmp[v] == a[i]]['target'].mean())
            
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
            z.append(tmp[(tmp[v] >= bound[i]) & (tmp[v] < bound[i+1])]['target'].mean())

        if np.sum(tmp[v].isnull())>0:
            x.append((i+1)*2)
            y.append(np.sum(tmp[v].isnull()))
            label.append("NaN")
            z.append(tmp[tmp[v].isnull()]['target'].mean())


    fig, ax1 = plt.subplots()
    plt.title(v+":"+sigName[v])
    ax2 = ax1.twinx()
    ax1.bar(x, y, 1/1.5, color="blue",align='center')
    ax2.plot(x,z,linestyle='-',marker='o',color="red")
    fig.autofmt_xdate()
    plt.xticks(x,label)
    ax1.set_xlabel("Value Range")
    ax1.set_ylabel("Counts",color="blue")
    ax2.set_ylabel("Bad Rate",color="red")

    #fig.savefig("plots2/"+v+":"+sigName[v]+".png")
    fig.savefig(v+":"+sigName[v]+".png")
    plt.close(fig)

