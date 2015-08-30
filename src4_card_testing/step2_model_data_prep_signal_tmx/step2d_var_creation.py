import csv
import gzip
import os
import sys
import datetime
import random
from numpy import *
sys.path.append("/fraud_model/Code/tools/csv_operations")
import csv_ops
from csv_ops import *
from csv_woe_cat import *
from csv_impute import *
from multiprocessing import Pool

from scipy.spatial import distance

# weird transition like G->B
# how pronounceable 

#===============================================================================
# #gibrish and strange transition detection
#===============================================================================
import pickle
sys.path.append("/fraud_model/Code/Gibberish-Detector")
import gib_detect_train

class gib_detect:
    
    def __init__(self,model_file_name):
        #model_file_name='gib_model.pki'
        model_data = pickle.load(open(model_file_name, 'rb'))
        self.model_mat = model_data['mat']
        self.threshold = model_data['thresh']
        
    def score(self,input_str):
        score=gib_detect_train.avg_transition_prob(input_str, self.model_mat, self.threshold)
        isgib=(score>self.threshold)
        return score, isgib


gib=gib_detect('/fraud_model/Code/Gibberish-Detector/gib_model.p')
name_gib=gib_detect('/fraud_model/Code/Gibberish-Detector/name_gib_model.p')

#===============================================================================
# # string pattern and key distances
#===============================================================================
def diff(a,b):
    if a==b:
        return 1
    else:
        return 0
    
def longest_common_sequence(a,b):
    if len(a)==0:
        return 0
    elif len(b)==0:
        return 0
    
    return max(longest_common_sequence(a[1:],b),longest_common_sequence(a,b[1:]),longest_common_sequence(a[1:],b[1:])+diff(a[0],b[0]))
    
def longest_common_substr(S,T):
    m = len(S)
    n = len(T)
    counter = [[0]*(n+1) for x in range(m+1)]
    longest = 0
    lcs_set = set()
    for i in range(m):
        for j in range(n):
            if S[i] == T[j]:
                c = counter[i][j] + 1
                counter[i+1][j+1] = c
                if c > longest:
                    lcs_set = set()
                    longest = c
                    lcs_set.add(S[i-c+1:i+1])
                elif c == longest:
                    lcs_set.add(S[i-c+1:i+1])
    #return longest, lcs_set
    return longest

class keyboard:
    def __init__(self):
        # initialize keyboard layout and key coords
        self.keyboard=array([
                  ['1','2','3','4','5','6','7','8','9','0'],
                  ['q','w','e','r','t','y','u','i','o','p'],
                  ['a','s','d','f','g','h','j','k','l',';'],
                  ['z','x','c','v','b','n','m',',','.','/']
                  ])
    
        self.coords={}
        for lett in self.keyboard.ravel():
            tmp=where(self.keyboard==lett)
            self.coords[lett]=[tmp[0][0],tmp[1][0]]
        self.coords[' ']=[4,5]
    
        self.coords_corrected={}
        for i,row in enumerate(self.keyboard):
            for lett in row:
                tmp=where(self.keyboard==lett)
                self.coords_corrected[lett]=[tmp[0][0],tmp[1][0]]
                self.coords_corrected[lett][1]+=0.4*i
        self.coords_corrected[' ']=[4,5]
    
        # initialize special adjpatterns
        self.parallel=[]
        for row in self.keyboard:
            for i in range(len(row)-2):
                self.parallel.append(''.join(row[i:i+3]))
        
        self.parallel_reversed=[]
        for pattern in self.parallel:
            self.parallel_reversed.append(pattern[::-1])
        
        self.vertical=[]
        for i in range(len(self.keyboard[0])):
            tmp=''
            for j in range(len(self.keyboard)):
                tmp=tmp+self.keyboard[j][i]
            self.vertical.append(tmp)
        
        self.vertical_reversed=[]
        for pattern in self.vertical:
            self.vertical_reversed.append(pattern[::-1])
        
        #print self.parallel,self.parallel_reversed
        #print self.vertical,self.vertical_reversed
    
    
    def keyboard_distance(self,a,b):
        try:
            print self.coords[a.lower()],self.coords[b.lower()]
            return round(distance.euclidean(self.coords[a.lower()],self.coords[b.lower()]))
        except:
            return 6
        
    def keyboard_distance_corrected(self,a,b):
        try:
            return round(distance.euclidean(self.coords_corrected[a.lower()],self.coords_corrected[b.lower()]))
        except:
            return 6



#===============================================================================
# # signal creation
#===============================================================================
def word_signal_creation(row,prefix,word):
    # basic stats
    row[prefix+'len'] = len(word)
    row[prefix+'uniq_cnt'] = len(set(word))
    try:
        row[prefix+'uniq_frac'] = row[prefix+'uniq_cnt']/float(row[prefix+'len'])
    except:
        row[prefix+'uniq_frac'] = 1
    
    upper_cnt=0
    for lett in word:
        if lett.isupper():
            upper_cnt+=1
    row[prefix+'upper_cnt']=upper_cnt
    try:
        row[prefix+'upper_frac']=upper_cnt/float(row[prefix+'len'])
    except:
        row[prefix+'upper_frac']=0
        
    # keyboard distance related signals
    k=keyboard()
    dist_list=[]
    for i in range(len(word)-1):
        dist_list.append(k.keyboard_distance_corrected(word[i],word[i+1]))
    if dist_list==[]:
        dist_list=[1]
    try:
        row[prefix+'key_dist_1_frac'] = dist_list.count(1)/float(len(dist_list))
    except:
        row[prefix+'key_dist_1_frac'] = 1
        
    dist_list = array(dist_list)
    row[prefix+'key_dist_total'] = sum(dist_list)
    row[prefix+'key_dist_mean'] = mean(dist_list)
    row[prefix+'key_dist_median'] = median(dist_list)
    row[prefix+'key_dist_max'] = max(dist_list)
    row[prefix+'key_dist_min'] = min(dist_list)
    row[prefix+'key_dist_std'] = median(dist_list)
    
    # keyboard pattern related signals
    row[prefix+'pattern_parallel']=0
    for pattern in k.parallel:
        if pattern in word:
            row[prefix+'pattern_parallel']=1
            break
    
    row[prefix+'pattern_parallel_reversed']=0
    for pattern in k.parallel_reversed:
        if pattern in word:
            row[prefix+'pattern_parallel_reversed']=1
            break

#     row[prefix+'pattern_vertical']=0 # vertical pattern not seen
#     for pattern in k.vertical:
#         if pattern in word:
#             row[prefix+'pattern_vertical']=1
#             break
# 
#     row[prefix+'pattern_vertical_reversed']=0
#     for pattern in k.vertical_reversed:
#         if pattern in word:
#             row[prefix+'pattern_vertical_reversed']=1
#             break
        
    return row
        
# max len of common substring

# strange letter transition


all_field_names=['signal_353', 'signal_352', 'signal_351', 'signal_356', 'signal_355', 'signal_354', 'first_name_key_dist_total', 'user_name_gib_ind', 'last_name_gib_score', 'signal_616', 'signal_617', 'signal_614', 'signal_615', 'signal_612', 'signal_613', 'signal_610', 'signal_611', 'signal_618', 'signal_128', 'signal_129', 'signal_127', 'lo_tm_true_ip', 'signal_100066', 'signal_46', 'signal_47', 'signal_44', 'signal_45', 'signal_42', 'signal_43', 'payment_request_id', 'signal_100048', 'signal_100108', 'signal_48', 'signal_49', 'last_name_key_dist_max', 'last_name_uniq_cnt', 'signal_71', 'signal_70', 'common_len_user_name_last_name', 'lo_signal_506', 'signal_537', 'signal_536', 'signal_535', 'signal_534', 'signal_533', 'signal_532', 'signal_531', 'signal_530', 'signal_313', 'signal_539', 'signal_538', 'amount', 'signal_228', 'signal_542', 'signal_543', 'signal_540', 'signal_541', 'signal_546', 'signal_547', 'signal_544', 'signal_545', 'first_name_key_dist_1_frac', 'signal_548', 'type', 'first_name_key_dist_mean', 'signal_362', 'signal_363', 'signal_361', 'signal_364', 'user_name_pattern_parallel', 'lo_signal_620', 'common_len_user_name_first_name_frac', 'lo_signal_621', 'common_len_user_name_full_name', 'signal_649', 'signal_648', 'user_name_upper_frac', 'user_name_key_dist_1_frac', 'signal_641', 'signal_640', 'signal_643', 'signal_642', 'signal_645', 'signal_644', 'signal_647', 'signal_646', 'signal_100143', 'last_name_key_dist_std', 'signal_168', 'signal_169', 'signal_160', 'signal_161', 'signal_162', 'signal_163', 'signal_164', 'signal_165', 'signal_166', 'signal_167', 'common_len_user_name_first_name', 'signal_100139', 'signal_39', 'signal_38', 'signal_37', 'signal_36', 'signal_35', 'signal_34', 'signal_33', 'signal_100039', 'signal_31', 'user_name_uniq_cnt', 'user_name_key_dist_min', 'signal_595', 'signal_594', 'signal_597', 'signal_596', 'signal_591', 'signal_590', 'signal_593', 'signal_592', 'last_name_upper_frac', 'signal_508', 'signal_509', 'signal_506', 'signal_507', 'signal_504', 'signal_505', 'signal_503', 'signal_500', 'signal_501', 'user_name_uniq_frac', 'last_name_key_dist_total', 'signal_605', 'signal_604', 'signal_607', 'signal_606', 'signal_601', 'signal_600', 'signal_603', 'signal_602', 'signal_609', 'signal_608', 'signal_133', 'signal_132', 'signal_131', 'signal_130', 'signal_135', 'signal_134', 'last_name_bad', 'first_name_upper_cnt', 'last_name_key_dist_min', 'last_name_uniq_frac', 'signal_312', 'signal_79', 'signal_78', 'user_name_upper_cnt', 'signal_73', 'signal_72', 'signal_100072', 'signal_100073', 'signal_77', 'signal_76', 'signal_75', 'signal_74', 'last_name_len', 'lastName', 'user_name_key_dist_std', 'signal_100030', 'first_name_bad', 'signal_420', 'user_name_number_multi_seg', 'signal_429', 'signal_428', 'signal_425', 'signal_424', 'signal_427', 'signal_426', 'signal_421', 'signal_658', 'signal_423', 'signal_422', 'signal_659', 'last_name_key_dist_mean', 'lo_signal_156', 'signal_300', 'signal_653', 'lo_signal_600', 'signal_650', 'signal_651', 'target', 'signal_656', 'signal_657', 'first_name_uniq_cnt', 'signal_654', 'signal_655', 'user_name_key_dist_total', 'signal_40', 'signal_371', 'signal_41', 'manual_review', 'signal_100102', 'user_name_key_dist_mean', 'first_name_key_dist_std', 'first_name_pattern_parallel_reversed', 'device', 'user_name_number_frac', 'first_name_pattern_parallel', 'user_name_number_not_end', 'signal_179', 'signal_178', 'signal_177', 'signal_176', 'signal_175', 'signal_174', 'signal_173', 'last_name_key_dist_1_frac', 'signal_171', 'signal_170', 'first_name_gib_ind', 'lo_ip', 'domain_name', 'signal_20', 'first_name_key_dist_median', 'signal_100042', 'signal_24', 'signal_25', 'signal_26', 'signal_27', 'signal_28', 'signal_29', 'signal_100124', 'fs_payment_request_id', 'signal_519', 'signal_518', 'signal_515', 'signal_514', 'signal_517', 'signal_516', 'signal_511', 'signal_510', 'signal_513', 'signal_512', 'last_name_upper_cnt', 'signal_638', 'signal_630', 'signal_631', 'signal_632', 'signal_633', 'signal_634', 'lo_signal_638', 'signal_636', 'signal_637', 'state', 'signal_204', 'email', 'signal_146', 'signal_147', 'signal_144', 'signal_145', 'signal_142', 'signal_143', 'signal_140', 'signal_141', 'signal_148', 'signal_149', 'first_name_key_dist_min', 'signal_708', 'signal_709', 'signal_704', 'signal_705', 'signal_706', 'signal_707', 'signal_700', 'signal_701', 'signal_702', 'signal_703', 'signal_100087', 'signal_100086', 'signal_68', 'signal_69', 'signal_100083', 'signal_64', 'signal_65', 'signal_66', 'signal_67', 'signal_61', 'signal_62', 'signal_63', 'signal_100163', 'last_name_gib_ind', 'lo_signal_2', 'lo_signal_8', 'tm_device_id', 'common_len_user_name_full_name_frac', 'user_name', 'direction', 'signal_19', 'signal_18', 'signal_100018', 'signal_11', 'signal_10', 'signal_13', 'signal_12', 'signal_15', 'signal_14', 'signal_17', 'signal_16', 'signal_410', 'signal_411', 'signal_412', 'signal_413', 'signal_414', 'signal_415', 'signal_416', 'signal_417', 'signal_418', 'signal_419', 'tm_true_ip', 'last_name_key_dist_median', 'ip', 'signal_430', 'signal_248', 'signal_247', 'signal_560', 'signal_561', 'lo_lastName', 'user_name_gib_score', 'lo_firstName', 'common_len_user_name_last_name_frac', 'first_name_gib_score', 'target_ct', 'signal_100057', 'first_name_uniq_frac', 'user_name_key_dist_median', 'signal_182', 'signal_180', 'signal_181', 'user_name_len', 'signal_100119', 'signal_50', 'signal_100113', 'signal_100110', 'signal_59', 'signal_58', 'signal_100114', 'lo_signal_355', 'last_name_pattern_parallel', 'create_time', 'signal_520', 'signal_521', 'signal_522', 'signal_523', 'signal_524', 'signal_525', 'signal_526', 'signal_527', 'signal_528', 'signal_529', 'first_name_upper_frac', 'tm_fuzzy_device_id', 'user_name_number_cnt', 'lo_signal_610', 'lo_signal_622', 'signal_629', 'signal_628', 'signal_627', 'signal_626', 'signal_625', 'signal_624', 'signal_623', 'signal_622', 'signal_621', 'signal_620', 'first_name_key_dist_max', 'signal_155', 'signal_154', 'signal_157', 'signal_156', 'signal_151', 'signal_150', 'signal_153', 'signal_152', 'signal_159', 'signal_158', 'user_name_key_dist_max', 'signal_9', 'signal_8', 'signal_1', 'signal_2', 'signal_4', 'signal_713', 'signal_712', 'signal_711', 'signal_710', 'lo_signal_13', 'signal_715', 'signal_714', 'signal_652', 'signal_301', 'signal_302', 'signal_303', 'signal_304', 'signal_305', 'signal_306', 'signal_307', 'signal_100096', 'signal_100154', 'signal_100099', 'firstName', 'lo_email', 'first_name_len', 'lo_signal_548', 'signal_100024', 'signal_407', 'signal_406', 'signal_405', 'signal_404', 'signal_403', 'signal_402', 'signal_401', 'signal_400', 'signal_409', 'signal_408', 'signal_586', 'signal_584', 'signal_585', 'signal_582', 'signal_583', 'signal_580', 'signal_581', 'lo_signal_547', 'signal_571', 'signal_570', 'user_name_pattern_parallel_reversed', 'last_name_pattern_parallel_reversed']\
+['first_name_gib_name_score','last_name_gib_name_score','user_name_gib_name_score']

def var_creation(input_file,output_file):

    infile=gzip.open(input_file,'rb')
    incsv=csv.DictReader(infile)
    header=incsv.fieldnames
    header_additional=[]
    for var in all_field_names:
        if var not in header:
            header_additional.append(var)
    header_out=header+sorted(header_additional)
    
    outfile=gzip.open(output_file,'w')
    outcsv=csv.writer(outfile)
    outcsv.writerow(header_out)
    
    random.seed(1)
    nRow=0
    for row in incsv:
        # single word stats
        row = word_signal_creation(row,prefix='first_name_',word=row['firstName'])
        row = word_signal_creation(row,prefix='last_name_',word=row['lastName'])
        
        tmp=row['email'].split('@')
        if len(tmp)>=2:
            row['user_name']=tmp[0]
            row['domain_name']=tmp[1]
        else:
            row['user_name']=''
            row['domain_name']=''
        row = word_signal_creation(row,prefix='user_name_',word=row['user_name'])
        
        # common string len stats
        len_first_name=float(len(row['firstName']))
        len_last_name=float(len(row['lastName']))
        row['common_len_user_name_first_name']=longest_common_substr(row['user_name'],row['firstName'])
        if len_first_name>0:
            row['common_len_user_name_first_name_frac']=row['common_len_user_name_first_name']/len_first_name
        else:
            row['common_len_user_name_first_name_frac']=0
        
        row['common_len_user_name_last_name']=longest_common_substr(row['user_name'],row['lastName'])
        if len_last_name>0:
            row['common_len_user_name_last_name_frac']=row['common_len_user_name_last_name']/len_last_name
        else:
            row['common_len_user_name_last_name_frac']=0
        
        row['common_len_user_name_full_name']=row['common_len_user_name_first_name']+row['common_len_user_name_last_name']
        if len_first_name+len_last_name>0:
            row['common_len_user_name_full_name_frac']=row['common_len_user_name_full_name']/(len_first_name+len_last_name)
        else:
            row['common_len_user_name_full_name_frac']=0
            
        # number stats
        number_index_list=[]
        for i,lett in enumerate(row['user_name']):
            try:
                int(lett)
                number_index_list.append(i)
            except:
                continue
        row['user_name_number_cnt']=len(number_index_list)
        if len(row['user_name'])>0:
            row['user_name_number_frac']=row['user_name_number_cnt']/float(len(row['user_name']))
        else:
            row['user_name_number_frac']=0
        
        row['user_name_number_not_end']=0
        row['user_name_number_multi_seg']=0
        if row['user_name_number_cnt']>0:
            if number_index_list[-1]<len(row['user_name'])-1:
                row['user_name_number_not_end']=1
                #print number_index_list,row['user_name'] #test
            if number_index_list[-1]-number_index_list[0]>len(number_index_list)-1:
                row['user_name_number_multi_seg']=1
                #print row['user_name'] #test
        
        # indicators
        if row['firstName']=="Elisabeth":
            row['first_name_bad']=1
        else:
            row['first_name_bad']=0
            
        if row['lastName']=="Pedder":
            row['last_name_bad']=1
        else:
            row['last_name_bad']=0
            
        # gibrish detection
        gibscore,isgib=gib.score(row['firstName'])
        row['first_name_gib_score']=gibscore
        row['first_name_gib_ind']=int(isgib)
        gibscore,isgib=gib.score(row['lastName'])
        row['last_name_gib_score']=gibscore
        row['last_name_gib_ind']=int(isgib)
        gibscore,isgib=gib.score(row['user_name'])
        row['user_name_gib_score']=gibscore
        row['user_name_gib_ind']=int(isgib)
        
        gibscore,isgib=gib.score(row['firstName'])
        row['first_name_gib_name_score']=gibscore
        gibscore,isgib=gib.score(row['lastName'])
        row['last_name_gib_name_score']=gibscore
        gibscore,isgib=gib.score(row['user_name'])
        row['user_name_gib_name_score']=gibscore
            
        # ouput to file
        outcsv.writerow([row[var] for var in header_out])
        
        
        if nRow==1:
            print row.keys()
        
        nRow+=1
        if nRow%100000 ==0:
            print nRow," rows are processed"


def var_creation_helper(arg):
    var_creation(arg[0],arg[1])



global work_dir

if len(sys.argv) <=1:
    work_dir='/fraud_model/Data/card_testing/' # everything should/will be in w
    #support_dir='/fraud_model/Code/src4_card_testing/support_files/'
elif len(sys.argv) ==3:
    work_dir=sys.argv[1]
    #support_dir=sys.argv[2]
else:
    print "stdin input should be 0 or 1 vars, 0 using data location in code, 1 using input."


################################################################################
# Perform imputation                                                           #
# 1. Calculate statistics, create missing value mapping                        #
################################################################################

# input_file=work_dir+"model_data_pmt_ins_ds_imp_woe.csv.gz" # in sample data used to calculate the stats
# output_file=work_dir+"model_data_pmt_ins_ds_imp_woe_vc.csv.gz" # in sample data used to calculate the stats
# var_creation(input_file,output_file)


################################################################################
# process
################################################################################
input_list = (
              [work_dir+"model_data_pmt_ins_ds_imp_woe.csv.gz",work_dir+"model_data_pmt_ins_ds_imp_woe_vc.csv.gz"], 
              [work_dir+"model_data_pmt_oos_ds_imp_woe.csv.gz",work_dir+"model_data_pmt_oos_ds_imp_woe_vc.csv.gz"],
              [work_dir+"test_data_1mo_pmt_ds_imp_woe.csv.gz",work_dir+"test_data_1mo_pmt_ds_imp_woe_vc.csv.gz"],
              [work_dir+"test_data_2mo_pmt_ds_imp_woe.csv.gz",work_dir+"test_data_2mo_pmt_ds_imp_woe_vc.csv.gz"],
              [work_dir+"test_data_3mo_pmt_ds_imp_woe.csv.gz",work_dir+"test_data_3mo_pmt_ds_imp_woe_vc.csv.gz"],
              [work_dir+"test_data_4mo_pmt_ds_imp_woe.csv.gz",work_dir+"test_data_4mo_pmt_ds_imp_woe_vc.csv.gz"],
              [work_dir+"test_data_5mo_pmt_ds_imp_woe.csv.gz",work_dir+"test_data_5mo_pmt_ds_imp_woe_vc.csv.gz"],
              [work_dir+"test_data_6mo_pmt_ds_imp_woe.csv.gz",work_dir+"test_data_6mo_pmt_ds_imp_woe_vc.csv.gz"],
              )
            # Inputs: impute_replace_pickle(work_dir,input_file,output_file)
pool = Pool(processes=8)
pool.map(var_creation_helper, input_list)

csv_EDD(work_dir+"model_data_pmt_ins_ds_imp_woe_vc.csv.gz")
