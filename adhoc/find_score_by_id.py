import csv
import gzip
import os
import sys
import time
import random
from numpy import *
sys.path.append("/Users/junhe/Documents/workspace/fraud_model/src/csv_operations")
sys.path.append("/Users/junhe/Documents/workspace/fraud_model/prod")

from csv_ops import *


file1= '/Users/junhe/Documents/Results/Model_Results_Signal_Tmx/score_dec_full.csv'
file2= '/Users/junhe/Documents/Data/Model_Data_Signal_Tmx/Canadian_collusion.csv'
key_list=['payment_request_id']
file_out = '/Users/junhe/Documents/Results/Model_Results_Signal_Tmx/score_dec_CA_collusion.csv'

csv_leftjoin(file1, file2, key_list, file_out, tokeep='11')