import pandas as pd
import numpy as np
import copy as cp


#===============================================================================
# ####################### for payment model ##########################
#===============================================================================

global df, direction
direction=1 # = 1 for 'pmt' or 2 for 'wd'
type='pmt' # = 'pmt' or 'wd'


############# load and preapre data #####################
out_dir = '/fraud_model/Results/model_rule_optimization/ad_hoc/'
result_file = out_dir + 'score_rule_profiling_'+type+'.csv'
# load data
df=pd.read_csv('/fraud_model/Data/model_rule_optimization/rule_score_'+type+'_20150518_20150522.csv')

# fill Nan
df.blacklist_reason.fillna("Good",inplace=True)
df.fillna(0,inplace=True)

# filter data by direction and blacklist reason in (Fraud, Good)
df = df[(df.direction == direction) * ((df.blacklist_reason =='Fraud') + (df.blacklist_reason =='Good'))]

# add one column for cnt
df['n'] = np.array([1]*len(df))

    
dollar_band = np.array(['ge100000']*len(df))

nRow=0
for i in range(len(df)):
    #print i,df['amount'].iloc[i]
    if df['amount'].iloc[i]<100:
        dollar_band[i]=100
    elif df['amount'].iloc[i]<500:
        dollar_band[i]=500
    elif df['amount'].iloc[i]<1000:
        dollar_band[i]=1000
    elif df['amount'].iloc[i]<2000:
        dollar_band[i]=2000
    elif df['amount'].iloc[i]<5000:
        dollar_band[i]=5000
    elif df['amount'].iloc[i]<10000:
        dollar_band[i]=10000
    elif df['amount'].iloc[i]<20000:
        dollar_band[i]=20000
    elif df['amount'].iloc[i]<50000:
        dollar_band[i]=50000
    elif df['amount'].iloc[i]<100000:
        dollar_band[i]=100000
    
    nRow += 1
    if nRow % 10000 ==0:
        print nRow
        
df['dollar_band']=dollar_band

df2 = df[['score','amount','score_band','dollar_band',
          'rule_36',
          'rule_127',
          'rule_30',
          'rule_322',
          'rule_168',
          'rule_412',
          'rule_130',
          'rule_39',
          'rule_245',
          'rule_48',
          'rule_422',
          'rule_54',
          'rule_42',
          'rule_453',
          'rule_159',
          'rule_387',
          'rule_392',
          'rule_145',
          'score_ge_55',
          'blacklist_reason','n']]

df2.to_csv(result_file)

#pd.pivot_table(df,values=['n'],index=['score_band','dollar_band'],columns=['blacklist_reason'], aggfunc=np.sum)





#===============================================================================
# ####################### for withdrawal model ##########################
#===============================================================================


global df, direction
direction=2 # = 1 for 'pmt' or 2 for 'wd'
type='wd' # = 'pmt' or 'wd'


############# load and preapre data #####################
out_dir = '/fraud_model/Results/model_rule_optimization/ad_hoc/'
result_file = out_dir + 'score_rule_profiling_'+type+'.csv'
# load data
df=pd.read_csv('/fraud_model/Data/model_rule_optimization/rule_score_'+type+'_20150518_20150522.csv')

# fill Nan
df.blacklist_reason.fillna("Good",inplace=True)
df.fillna(0,inplace=True)

# filter data by direction and blacklist reason in (Fraud, Good)
df = df[(df.direction == direction) * ((df.blacklist_reason =='Fraud') + (df.blacklist_reason =='Good'))]

# add one column for cnt
df['n'] = np.array([1]*len(df))

    
dollar_band = np.array(['ge100000']*len(df))

nRow=0
for i in range(len(df)):
    #print i,df['amount'].iloc[i]
    if df['amount'].iloc[i]<100:
        dollar_band[i]=100
    elif df['amount'].iloc[i]<500:
        dollar_band[i]=500
    elif df['amount'].iloc[i]<1000:
        dollar_band[i]=1000
    elif df['amount'].iloc[i]<2000:
        dollar_band[i]=2000
    elif df['amount'].iloc[i]<5000:
        dollar_band[i]=5000
    elif df['amount'].iloc[i]<10000:
        dollar_band[i]=10000
    elif df['amount'].iloc[i]<20000:
        dollar_band[i]=20000
    elif df['amount'].iloc[i]<50000:
        dollar_band[i]=50000
    elif df['amount'].iloc[i]<100000:
        dollar_band[i]=100000
    
    nRow += 1
    if nRow % 10000 ==0:
        print nRow
        
df['dollar_band']=dollar_band

df2 = df[['payment_request_id',
          'score','amount','score_band','dollar_band',
          'rule_96',
          'rule_367',
          'rule_407',
          'rule_201',
          'rule_120',
          'rule_78',
          'rule_402',
          'rule_99',
          'rule_179',
          'rule_240',
          'rule_397',
          'rule_270',
          'rule_27',
          'rule_165',
          'rule_63',
          'rule_265',
          'rule_230',
          'rule_60',
          'rule_235',
          'rule_225',
          'score_ge_15',
          'blacklist_reason','n']]

df2.to_csv(result_file)
