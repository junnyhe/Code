import csv
import gzip
import sys
import time
import pickle
from numpy import *
import random

from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier


#sys.path.append("/Users/junhe/Documents/workspace/fraud_model/src/model_wd_tools")
#sys.path.append("/Users/junhe/Documents/workspace/fraud_model/src/csv_operations")


class model_scoring:
    def __init__(self,impute_value_filename,risk_table_filename,var_list_filename,model_filename):
        ######################## load all the required variable ############################
        t0=time.time()
        # load imputation mapping table from pickle
        self.impute_values = pickle.load(open(impute_value_filename,'rb'))
        
        # load risk table from pickle
        self.risk_table, self.woe_var_list = pickle.load(open(risk_table_filename,'rb'))

        # load modeling variables from csv
        varlist_file=open(var_list_filename,'rU')
        varlist_csv=csv.reader(varlist_file)
        self.var_list=[]
        for row in varlist_csv:
            self.var_list.append(row[0])
        
        # load model from pickle  
        self.model = pickle.load(open(model_filename,'rb'))
        
        print "model_scoring object is created", time.time()-t0, "sec lapsed"


    def score(self,data):
        ########################### data prep ################################
        
        # impute replace
        imp_var_list=self.impute_values.keys()
        for var in imp_var_list:
            try:
                float(data[var])
            except:
                data[var]=self.impute_values[var]
        
        # woe assign
        woe_var_list=self.woe_var_list
        for var_name in woe_var_list:
            try:
                data['lo_'+var_name]= self.risk_table[var_name][data[var_name]]['log_odds_sm']
            except:
                data['lo_'+var_name]= self.risk_table[var_name]['default']['log_odds_sm']
                # print 'Warning: new value of the variable: ',var_name,'=',data[var_name],' is not found in the risk table. Default log_odds of overall population is assigned.'
        
        
        ########################### scoring ####################################
        
        X=array([float(data[var]) for var in self.var_list])
        
        p_pred = self.model.predict_proba(X)
        p_pred = p_pred[:,1][0]
        
        return p_pred



################ test: compare scores with dev code ##################
if __name__=="__main__2":
    #initialize scoring instance
    support_file_dir='../../ProdSupport/prod2_withdrawal_signalonly_newest_time/'
    impute_value_filename = support_file_dir+'impute_values.p'
    risk_table_filename = support_file_dir+'risk_table.p'
    var_list_filename= support_file_dir+'model_var_list_signal.csv'
    model_filename= support_file_dir+'model.p'
    
    M=model_scoring(impute_value_filename,risk_table_filename,var_list_filename,model_filename)
    
    # load model dev results for comparison
    pt_pred=pickle.load(open("/home/junhe/fraud_model/Results/Model_Results_Signal_Only_v2wd_woeSmth=0_newest_time/RandomForest_signal_ds=0.2/pv_pred.p",'rb'))
    
    # test with oos data
    work_dir='/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v2wd_signalonly_newest_time/'
    input_file='model_data_wd_oos_ds.csv.gz'
    
    infile=gzip.open(work_dir+input_file,'rb')
    incsv=csv.DictReader(infile)
    p=[]
    nRow=0
    for data in incsv:
        p.append(M.score(data))
        score =  M.score(data)
        
        if score >0.02:
            #print data
            print pt_pred[nRow],score
        
        if pt_pred[nRow] != score:
            print "discrepancy found, attention !!!..........."
    
        nRow+=1
        if nRow % 2000==0:
            print nRow, "rows are processed"    
    
    # compare with oos scored with dev code after finish
    p_comp = zip(p,pt_pred)
    for row in p_comp:
        print row
        

from flask import Flask, request
class MyServer(Flask):

    def __init__(self, *args, **kwargs):
        super(MyServer, self).__init__(*args, **kwargs)
        support_file_dir='../../ProdSupport/prod2_withdrawal_signalonly_newest_time/'
        impute_value_filename = support_file_dir+'impute_values.p'
        risk_table_filename = support_file_dir+'risk_table.p'
        var_list_filename= support_file_dir+'model_var_list_signal.csv'
        model_filename= support_file_dir+'model.p'
        
        M=model_scoring(impute_value_filename,risk_table_filename,var_list_filename,model_filename)
        
        self.model =model_scoring(impute_value_filename,risk_table_filename,var_list_filename,model_filename)
        self.messages = []
app = MyServer(__name__)


@app.route("/model_wd" , methods = ["GET","POST"])
def hello():
    #request_data = {"signal_353": "", "signal_352": "", "signal_351": "", "signal_591": "", "signal_356": "", "signal_355": "", "signal_354": "", "signal_100086": "0", "signal_100087": "", "signal_100042": "0", "signal_24": 340.0, "signal_25": 0, "signal_26": 0, "signal_27": 0.0, "signal_28": 0.5, "signal_29": 0, "signal_69": "", "signal_614": "", "signal_593": "", "signal_508": "0", "signal_509": "0", "signal_506": "US", "signal_507": "0", "signal_504": "1", "signal_505": "1", "signal_503": "", "signal_500": "", "signal_501": "", "signal_182": 2.0, "signal_617": "", "signal_180": 2.0, "signal_181": 2.0, "signal_612": "", "signal_613": "", "signal_611": "", "signal_618": "", "signal_615": "", "signal_128": "20", "signal_129": "", "signal_127": 61.0, "fs_payment_request_id": "53132685", "signal_158": 3562.5900000000001, "signal_100048": "0", "signal_50": "", "signal_548": "", "signal_59": "8", "signal_58": "", "target": "0", "signal_100083": "0", "signal_519": "", "signal_518": "", "signal_515": "", "signal_514": "0", "signal_517": "", "signal_516": "", "signal_511": "0", "signal_510": "0", "signal_513": "0", "signal_512": "0", "signal_605": "", "signal_604": "", "signal_607": "", "signal_606": "", "signal_601": "", "signal_600": "90823", "signal_603": "", "signal_602": "", "signal_608": "", "blacklist_reason": "", "lo_signal_355": -3.4240129828861376, "signal_100066": "0.0", "signal_100073": "", "signal_79": "", "signal_46": "", "signal_47": "", "signal_44": "", "create_time": "1404176789", "signal_42": "", "signal_43": "", "payment_request_id": "53132685", "signal_41": "", "signal_100108": "0.0", "signal_48": "", "signal_49": "", "signal_520": "", "signal_521": "", "signal_522": "", "signal_523": "0", "signal_524": "1", "signal_525": "0", "signal_526": "0", "signal_527": "0", "signal_528": "0", "signal_529": "0", "lo_signal_13": -3.421266403951682, "state": "4", "signal_204": 1.0, "signal_100110": "0", "signal_100072": "0.0", "signal_146": "", "signal_147": "", "signal_144": 1.0, "signal_145": "", "signal_142": "1", "signal_143": "25.0", "signal_140": "525.0", "signal_141": 1.0, "signal_73": "", "signal_72": "", "signal_71": "", "signal_70": "", "signal_77": "", "signal_76": "", "signal_148": "0", "signal_149": "0", "lo_signal_506": -3.6341750126763639, "signal_75": "", "signal_74": "", "target2": "0", "signal_537": "0", "signal_536": "0", "signal_535": "0", "signal_534": "1", "signal_533": "2", "signal_532": "0", "signal_531": "0", "signal_530": "0", "lo_signal_600": 3.3184932618701031, "signal_539": "", "signal_538": "0", "signal_425": 0.006522, "signal_151": "", "signal_155": "100", "signal_154": "0", "signal_157": 6.0, "signal_156": "0", "signal_68": "", "signal_150": 20.0, "signal_153": "20", "signal_152": "0", "signal_64": "", "signal_65": "", "signal_66": "", "signal_67": "", "signal_159": "", "signal_61": "", "signal_62": "", "signal_63": "", "signal_9": "", "signal_8": "zoho.com", "signal_228": 637.0, "signal_429": 0.048275999999999999, "signal_428": 0.037499999999999999, "lo_signal_2": -4.0528002075508551, "signal_1": 0, "signal_424": 0.0051720000000000004, "signal_427": 0.0014989999999999999, "signal_2": "ok", "signal_421": 222.5, "signal_4": "", "signal_423": 0.0014250000000000001, "signal_422": 0.0037919999999999998, "signal_542": "", "signal_543": "", "signal_540": "", "signal_541": "", "signal_546": "", "signal_547": "", "signal_544": "", "signal_545": "", "signal_300": 2.0, "signal_301": 2.0, "signal_302": 2.0, "signal_303": 2.0, "signal_304": 10.0, "signal_305": 2.0, "signal_306": 2.0, "signal_307": 2.0, "signal_100096": "0", "direction": "1", "signal_592": "", "lo_signal_156": -4.0870433207620724, "signal_362": 0, "signal_361": 0, "signal_100099": "0.0", "signal_19": 0, "signal_18": 0, "signal_100018": "", "signal_11": 0, "signal_10": "", "signal_13": "Success", "signal_12": "100", "signal_15": "", "signal_14": "", "signal_17": 0, "signal_16": "", "signal_410": "330.0", "signal_411": "6.0", "signal_412": 0.0031359999999999999, "signal_413": 0.0012689999999999999, "signal_414": 0.0055880000000000001, "signal_415": 0.0052909999999999997, "signal_416": 0.0033909999999999999, "signal_417": 0.0015065, "signal_418": 0.048879500000000006, "signal_419": 0.055535000000000001, "signal_100057": "", "signal_45": "", "signal_78": "", "signal_313": "", "signal_312": "", "lo_signal_548": -3.1097395088778934, "signal_40": "", "signal_371": "0.0", "manual_review": "0", "lo_signal_8": 0.84729786038720312, "signal_100102": "0.0", "signal_100024": "0.0", "signal_407": 0.0011000000000000001, "signal_406": 0.0021580000000000002, "signal_405": 0.0039779999999999998, "signal_404": 0.0058650000000000004, "signal_403": 0.00088999999999999995, "signal_248": "7", "signal_401": "1.0", "signal_400": "500.0", "signal_426": 0.0033999999999999998, "signal_247": 335.0, "signal_409": 0.064516000000000004, "signal_408": 0.059812999999999998, "signal_168": "", "signal_169": "", "signal_560": "", "signal_561": "", "signal_160": "", "signal_161": "", "signal_162": "", "signal_163": "", "signal_164": "", "signal_165": "", "signal_166": "", "signal_167": "", "signal_580": "", "signal_100030": "0.0", "signal_39": "", "signal_38": 452.00049999999999, "signal_37": 80.814599999999999, "signal_36": "11.5644", "signal_35": "1", "signal_34": 2.5, "signal_33": 2.0, "signal_100039": "", "signal_31": 0, "signal_402": 0.0023890000000000001, "lo_signal_547": -3.635838276329272, "signal_420": 14971.5, "signal_616": "", "signal_571": "", "signal_570": "", "signal_590": "", "signal_179": 2.0, "signal_178": "", "signal_177": "", "signal_176": 2.0, "signal_175": "20", "signal_174": "0", "signal_173": 2.0, "signal_170": ""} 
    request_data = request.get_json(force=True)
    
    result = app.model.score(request_data)
    return '{"score":'+str(result)+'}'

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)



'''
from flask import Flask, request
import json
app = Flask(__name__)

@app.route("/model_wd", methods = ["GET","POST"])
def hello():
    
    support_file_dir='../../ProdSupport/prod2_withdrawal_signalonly_newest_time/'
    impute_value_filename = support_file_dir+'impute_values.p'
    risk_table_filename = support_file_dir+'risk_table.p'
    var_list_filename= support_file_dir+'model_var_list_signal.csv'
    model_filename= support_file_dir+'model.p'
    
    M=model_scoring(impute_value_filename,risk_table_filename,var_list_filename,model_filename)
    
    request_data = request.get_json(force=True)
    #request_data = {"signal_353": "", "signal_352": "", "signal_351": "", "signal_591": "", "signal_356": "", "signal_355": "", "signal_354": "", "signal_100086": "0", "signal_100087": "", "signal_100042": "0", "signal_24": 340.0, "signal_25": 0, "signal_26": 0, "signal_27": 0.0, "signal_28": 0.5, "signal_29": 0, "signal_69": "", "signal_614": "", "signal_593": "", "signal_508": "0", "signal_509": "0", "signal_506": "US", "signal_507": "0", "signal_504": "1", "signal_505": "1", "signal_503": "", "signal_500": "", "signal_501": "", "signal_182": 2.0, "signal_617": "", "signal_180": 2.0, "signal_181": 2.0, "signal_612": "", "signal_613": "", "signal_611": "", "signal_618": "", "signal_615": "", "signal_128": "20", "signal_129": "", "signal_127": 61.0, "fs_payment_request_id": "53132685", "signal_158": 3562.5900000000001, "signal_100048": "0", "signal_50": "", "signal_548": "", "signal_59": "8", "signal_58": "", "target": "0", "signal_100083": "0", "signal_519": "", "signal_518": "", "signal_515": "", "signal_514": "0", "signal_517": "", "signal_516": "", "signal_511": "0", "signal_510": "0", "signal_513": "0", "signal_512": "0", "signal_605": "", "signal_604": "", "signal_607": "", "signal_606": "", "signal_601": "", "signal_600": "90823", "signal_603": "", "signal_602": "", "signal_608": "", "blacklist_reason": "", "lo_signal_355": -3.4240129828861376, "signal_100066": "0.0", "signal_100073": "", "signal_79": "", "signal_46": "", "signal_47": "", "signal_44": "", "create_time": "1404176789", "signal_42": "", "signal_43": "", "payment_request_id": "53132685", "signal_41": "", "signal_100108": "0.0", "signal_48": "", "signal_49": "", "signal_520": "", "signal_521": "", "signal_522": "", "signal_523": "0", "signal_524": "1", "signal_525": "0", "signal_526": "0", "signal_527": "0", "signal_528": "0", "signal_529": "0", "lo_signal_13": -3.421266403951682, "state": "4", "signal_204": 1.0, "signal_100110": "0", "signal_100072": "0.0", "signal_146": "", "signal_147": "", "signal_144": 1.0, "signal_145": "", "signal_142": "1", "signal_143": "25.0", "signal_140": "525.0", "signal_141": 1.0, "signal_73": "", "signal_72": "", "signal_71": "", "signal_70": "", "signal_77": "", "signal_76": "", "signal_148": "0", "signal_149": "0", "lo_signal_506": -3.6341750126763639, "signal_75": "", "signal_74": "", "target2": "0", "signal_537": "0", "signal_536": "0", "signal_535": "0", "signal_534": "1", "signal_533": "2", "signal_532": "0", "signal_531": "0", "signal_530": "0", "lo_signal_600": 3.3184932618701031, "signal_539": "", "signal_538": "0", "signal_425": 0.006522, "signal_151": "", "signal_155": "100", "signal_154": "0", "signal_157": 6.0, "signal_156": "0", "signal_68": "", "signal_150": 20.0, "signal_153": "20", "signal_152": "0", "signal_64": "", "signal_65": "", "signal_66": "", "signal_67": "", "signal_159": "", "signal_61": "", "signal_62": "", "signal_63": "", "signal_9": "", "signal_8": "zoho.com", "signal_228": 637.0, "signal_429": 0.048275999999999999, "signal_428": 0.037499999999999999, "lo_signal_2": -4.0528002075508551, "signal_1": 0, "signal_424": 0.0051720000000000004, "signal_427": 0.0014989999999999999, "signal_2": "ok", "signal_421": 222.5, "signal_4": "", "signal_423": 0.0014250000000000001, "signal_422": 0.0037919999999999998, "signal_542": "", "signal_543": "", "signal_540": "", "signal_541": "", "signal_546": "", "signal_547": "", "signal_544": "", "signal_545": "", "signal_300": 2.0, "signal_301": 2.0, "signal_302": 2.0, "signal_303": 2.0, "signal_304": 10.0, "signal_305": 2.0, "signal_306": 2.0, "signal_307": 2.0, "signal_100096": "0", "direction": "1", "signal_592": "", "lo_signal_156": -4.0870433207620724, "signal_362": 0, "signal_361": 0, "signal_100099": "0.0", "signal_19": 0, "signal_18": 0, "signal_100018": "", "signal_11": 0, "signal_10": "", "signal_13": "Success", "signal_12": "100", "signal_15": "", "signal_14": "", "signal_17": 0, "signal_16": "", "signal_410": "330.0", "signal_411": "6.0", "signal_412": 0.0031359999999999999, "signal_413": 0.0012689999999999999, "signal_414": 0.0055880000000000001, "signal_415": 0.0052909999999999997, "signal_416": 0.0033909999999999999, "signal_417": 0.0015065, "signal_418": 0.048879500000000006, "signal_419": 0.055535000000000001, "signal_100057": "", "signal_45": "", "signal_78": "", "signal_313": "", "signal_312": "", "lo_signal_548": -3.1097395088778934, "signal_40": "", "signal_371": "0.0", "manual_review": "0", "lo_signal_8": 0.84729786038720312, "signal_100102": "0.0", "signal_100024": "0.0", "signal_407": 0.0011000000000000001, "signal_406": 0.0021580000000000002, "signal_405": 0.0039779999999999998, "signal_404": 0.0058650000000000004, "signal_403": 0.00088999999999999995, "signal_248": "7", "signal_401": "1.0", "signal_400": "500.0", "signal_426": 0.0033999999999999998, "signal_247": 335.0, "signal_409": 0.064516000000000004, "signal_408": 0.059812999999999998, "signal_168": "", "signal_169": "", "signal_560": "", "signal_561": "", "signal_160": "", "signal_161": "", "signal_162": "", "signal_163": "", "signal_164": "", "signal_165": "", "signal_166": "", "signal_167": "", "signal_580": "", "signal_100030": "0.0", "signal_39": "", "signal_38": 452.00049999999999, "signal_37": 80.814599999999999, "signal_36": "11.5644", "signal_35": "1", "signal_34": 2.5, "signal_33": 2.0, "signal_100039": "", "signal_31": 0, "signal_402": 0.0023890000000000001, "lo_signal_547": -3.635838276329272, "signal_420": 14971.5, "signal_616": "", "signal_571": "", "signal_570": "", "signal_590": "", "signal_179": 2.0, "signal_178": "", "signal_177": "", "signal_176": 2.0, "signal_175": "20", "signal_174": "0", "signal_173": 2.0, "signal_170": ""} 
    result = M.score(request_data)
    return '{"score":'+str(result)+'}'

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)
'''

