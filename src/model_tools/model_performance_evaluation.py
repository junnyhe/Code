import csv
from getAUC import *
from ks_roc import *
import matplotlib.pyplot as pl
import pickle
'''
#for comparison with sklearn
import numpy as np
from sklearn import metrics
'''

def performance_eval_train_validation(y,p_pred,yv,pv_pred,result_dir,output_suffix):
    # evaluate model performance on two data samples, train and test
    
    # Compute KS, ROC curve and AUC for train
    ks, ks_pos, pctl, tpr, fpr, tp_cumcnt, fp_cumcnt, threshold, lorenz_curve, lorenz_curve_capt_rate= ks_roc(y,p_pred)
    auc = getAUC(p_pred,y)
    
    '''
    #for comparison with sklearn
    fprx, tprx, thresholds = metrics.roc_curve(y, p_pred)
    auc2=metrics.auc(fprx, tprx)
    print auc,auc2
    '''
    # Compute KS, ROC curve and AUC for validation
    ks_v, ks_pos_v, pctl_v, tpr_v, fpr_v, tp_cumcnt_v, fp_cumcnt_v, threshold_v, lorenz_curve_v, lorenz_curve_capt_rate_v = ks_roc(yv,pv_pred)
    auc_v = getAUC(pv_pred,yv)
    
    '''
    #for comparison with sklearn
    fpry, tpry, thresholds = metrics.roc_curve(yv, pv_pred)
    auc_v2=metrics.auc(fpry, tpry)
    print auc_v,auc_v2
    '''
    
    # Plot ROC
    pl.figure(1, figsize=(8,8))
    pl.clf()
    pl.plot(fpr, tpr, 'b-', label='ROC Train(AUC=%0.3f)' % auc)
    pl.plot(fpr_v, tpr_v, 'r--', label='ROC Validation (AUC=%0.3f)' % auc_v)
    pl.plot([0, 1], [0, 1], 'k--')
    pl.xlim([0.0, 1.0])
    pl.ylim([0.0, 1.0])
    pl.xlabel('False Positive Rate')
    pl.ylabel('True Positive Rate')
    pl.title('ROC Curve: '+ output_suffix+ ' Model')
    pl.legend(loc="lower right")
    #pl.show()
    pl.savefig(result_dir+'ROC_AUC_'+output_suffix+'.png', bbox_inches='tight')
    
    # Plot Lorenz
    pl.figure(2, figsize=(8,8))
    pl.clf()
    pl.plot(pctl, tpr, 'b-',label='Fraud Train',linewidth=2.0 )
    pl.plot(pctl, fpr, 'g-',label='Non-Fraud Train',linewidth=2.0)
    pl.plot(pctl_v, tpr_v, 'm--',label='Fraud Validation' )
    pl.plot(pctl_v, fpr_v, 'r--',label='Non-Fraud Validation')
    pl.plot([0, 1], [0, 1], 'k--')
    pl.xlim([0.0, 1.0])
    pl.ylim([0.0, 1.0])
    pl.xlabel('Score Percentile')
    pl.ylabel('Cumulative Rate')
    tilte_lorenz="Lorenz Curve:KS Train={:.3f}({:.3f});KS Vali={:.3f}({:.3f})".format(float(ks),float(ks_pos),float(ks_v),float(ks_pos_v))
    pl.title(tilte_lorenz)
    pl.legend(loc="lower right")
    #pl.show()
    pl.savefig(result_dir+'Lorenz_'+output_suffix+'.png', bbox_inches='tight')
    
    # save results to disk
    pickle.dump(pv_pred,open(result_dir+"pv_pred.p",'wb'))
    pickle.dump(yv,open(result_dir+"yv.p",'wb'))
    
    
    ############# # Output validation Lorenz and KS results to file ##############
    
    out_file = open(result_dir + 'KS_validation_' + str(output_suffix)+'.csv', 'wb')
    out_csv = csv.writer(out_file)
    
    out_csv.writerow(["KS_"+str(output_suffix)])
    out_csv.writerow([ks_v])
    out_csv.writerow(["KS_Position_for_"+str(output_suffix)])
    out_csv.writerow([ks_pos_v])
    out_csv.writerow(["Capture_Rate_"+str(output_suffix)])
    for row in lorenz_curve_capt_rate_v:
        out_csv.writerow(row)
    out_csv.writerow(["Lorenz_Curve_"+str(output_suffix)])
    for row in lorenz_curve_v:
        out_csv.writerow(row)
        
    out_file.close()
        
        
    # Print results for all classifier
    print (output_suffix,'AUC on Validation','KS on Validation')
    print (output_suffix,auc_v,ks_v)


def performance_eval_test(y,p_pred,result_dir,output_suffix):
    # evaluate model performance on two data samples, train and test
    
    # Compute KS, ROC curve and AUC for train
    ks, ks_pos, pctl, tpr, fpr, tp_cumcnt, fp_cumcnt, threshold, lorenz_curve, lorenz_curve_capt_rate= ks_roc(y,p_pred)
    auc = getAUC(p_pred,y)

    
    # Plot ROC
    pl.figure(1, figsize=(8,8))
    pl.clf()
    pl.plot(fpr, tpr, 'b-', label='ROC Train(AUC=%0.3f)' % auc)
    pl.plot([0, 1], [0, 1], 'k--')
    pl.xlim([0.0, 1.0])
    pl.ylim([0.0, 1.0])
    pl.xlabel('False Positive Rate')
    pl.ylabel('True Positive Rate')
    pl.title('ROC Curve: '+ output_suffix+ ' Model')
    pl.legend(loc="lower right")
    #pl.show()
    pl.savefig(result_dir+'ROC_AUC_'+output_suffix+'.png', bbox_inches='tight')
    
    # Plot Lorenz
    pl.figure(2, figsize=(8,8))
    pl.clf()
    pl.plot(pctl, tpr, 'b-',label='Fraud Train',linewidth=2.0 )
    pl.plot(pctl, fpr, 'g-',label='Non-Fraud Train',linewidth=2.0)
    pl.plot([0, 1], [0, 1], 'k--')
    pl.xlim([0.0, 1.0])
    pl.ylim([0.0, 1.0])
    pl.xlabel('Score Percentile')
    pl.ylabel('Cumulative Rate')
    tilte_lorenz="Lorenz Curve:KS Test={:.3f}({:.3f})".format(float(ks),float(ks_pos))
    pl.title(tilte_lorenz)
    pl.legend(loc="lower right")
    #pl.show()
    pl.savefig(result_dir+'Lorenz_'+output_suffix+'.png', bbox_inches='tight')
    
    
    ############# # Output validation Lorenz and KS results to file ##############
    
    out_file = open(result_dir + 'KS_validation_' + str(output_suffix)+'.csv', 'wb')
    out_csv = csv.writer(out_file)
    
    out_csv.writerow(["KS_"+str(output_suffix)])
    out_csv.writerow([ks])
    out_csv.writerow(["KS_Position_for_"+str(output_suffix)])
    out_csv.writerow([ks_pos])
    out_csv.writerow(["Capture_Rate_"+str(output_suffix)])
    for row in lorenz_curve_capt_rate:
        out_csv.writerow(row)
    out_csv.writerow(["Lorenz_Curve_"+str(output_suffix)])
    for row in lorenz_curve:
        out_csv.writerow(row)
        
    out_file.close()
        
        
    # Print results for all classifier
    print (output_suffix,'AUC on test','KS on test')
    print (output_suffix,auc,ks)
