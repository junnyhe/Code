set -e
log_dir=/fraud_model/Code/

perf_result_pmt_signal=/fraud_model/Results/Model_Results_Signal_Only_v2pmt_woeSmth=0_newest_time/RandomForest_signal/KS_validation_RandomForest_signal_test_2mo.csv
perf_result_wd_signal=/fraud_model/Results/Model_Results_Signal_Only_v2wd_woeSmth=0_newest_time/RandomForest_signal/KS_validation_RandomForest_signal_test_2mo.csv
perf_result_pmt_signaltmx=/fraud_model/Results/Model_Results_Signal_Tmx_v3pmt_woeSmth=0_newest_time/RandomForest_signal_rc_tmxrc_ind/KS_validation_RandomForest_signal_rc_tmxrc_ind_test_2mo.csv
perf_result_wd_signaltmx=/fraud_model/Results/Model_Results_Signal_Tmx_v3wd_woeSmth=0_newest_time/RandomForest_signal_rc_tmxrc_ind/KS_validation_RandomForest_signal_rc_tmxrc_ind_test_2mo.csv

################### check model performance and send email #######################
# get precision at 70% recall
prec_at_recall_70_pmt_signal=$(sed '24q;d' $perf_result_pmt_signal | cut -d, -f9 | tr -d '\n' | tr -d '\r')
prec_at_recall_70_wd_signal=$(sed '24q;d' $perf_result_wd_signal | cut -d, -f9 | tr -d '\n' | tr -d '\r')
prec_at_recall_70_pmt_signaltmx=$(sed '24q;d' $perf_result_pmt_signaltmx | cut -d, -f9 | tr -d '\n' | tr -d '\r')
prec_at_recall_70_wd_signaltmx=$(sed '24q;d' $perf_result_wd_signaltmx | cut -d, -f9 | tr -d '\n' | tr -d '\r')


# prepare test pass/fail email message to send
performance_test_msg=""

if [ $(bc <<< "$prec_at_recall_70_pmt_signal > 0.2") -eq 1 ]; then
	performance_test_msg=${performance_test_msg}"pmt signal model performance test (prec_at_recall_70>0.2) PASSED: "$prec_at_recall_70_pmt_signal
else
	performance_test_msg=${performance_test_msg}"pmt signal model performance test (prec_at_recall_70>0.2) FAILED: "$prec_at_recall_70_pmt_signal
fi

if [ $(bc <<< "$prec_at_recall_70_wd_signal > 0.45") -eq 1 ]; then
	performance_test_msg=${performance_test_msg}"\nwd signal model performance test (prec_at_recall_70>0.45) PASSED: "$prec_at_recall_70_wd_signal
else
	performance_test_msg=${performance_test_msg}"\nwd signal model performance test (prec_at_recall_70>0.45) FAILED: "$prec_at_recall_70_wd_signal
fi

if [ $(bc <<< "$prec_at_recall_70_pmt_signaltmx > 0.2") -eq 1 ]; then
	performance_test_msg=${performance_test_msg}"\npmt signaltmx model performance test (prec_at_recall_70>0.2) PASSED: "$prec_at_recall_70_pmt_signaltmx
else
	performance_test_msg=${performance_test_msg}"\npmt signaltmx model performance test (prec_at_recall_70>0.2) FAILED: "$prec_at_recall_70_pmt_signaltmx
fi

if [ $(bc <<< "$prec_at_recall_70_wd_signaltmx > 0.45") -eq 1 ]; then
	performance_test_msg=${performance_test_msg}"\nwd signaltmx model performance test (prec_at_recall_70>0.45) PASSED: "$prec_at_recall_70_wd_signaltmx
else
	performance_test_msg=${performance_test_msg}"\nwd signaltmx model performance test (prec_at_recall_70>0.45) FAILED: "$prec_at_recall_70_wd_signaltmx
fi


echo -e $performance_test_msg>> ${log_dir}log_deploy.txt
if [[ $performance_test_msg =~ "FAIL" ]]; then
    title="Model Performance Test Failed"
    mutt -s "$title" -a "$perf_result_pmt_signal" -a "$perf_result_wd_signal" -a "$perf_result_pmt_signaltmx" -a "$perf_result_wd_signaltmx" -- junhe@wepay.com < ${log_dir}log_deploy.txt
    echo "["$(date)"] Performance test fail, program exit ! ..." >> ${log_dir}log_deploy.txt
    exit
else
    title="Model Performance Test Passed"
    #mutt -s "$title" -a "$perf_result_pmt_signal" -a "$perf_result_wd_signal" -a "$perf_result_pmt_signaltmx" -a "$perf_result_wd_signaltmx" -- junnyhe@gmail.com < ${log_dir}log_deploy.txt >> ${log_dir}log_deploy.txt
fi



################### copy files and test : payment model signal only ##################
echo "["$(date)"] Payment model signal only deployment starts ..." >> ${log_dir}log_deploy.txt

prod_dir=/code/model_scoring/
prod_support_dir=/code/model_scoring/payment_signalonly/
model_dir=/fraud_model/Results/Model_Results_Signal_Only_v2pmt_woeSmth=0_newest_time/RandomForest_signal/
impwoe_dir=/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt_signalonly_newest_time/

cp ${model_dir}model.p    $prod_support_dir
gzip ${prod_support_dir}model.p -f
cp ${model_dir}trivial_input_values.p    $prod_support_dir
cp ${impwoe_dir}risk_table.p    $prod_support_dir
cp ${impwoe_dir}impute_values.p    $prod_support_dir
python ${prod_dir}model_scoring_prod.py 1 >>${log_dir}log_deploy.txt  2>&1

echo "["$(date)"] Payment model signal only deployment is done ..." >> ${log_dir}log_deploy.txt



################### copy files and test : withdrawal model signal only ##################
echo "["$(date)"] Withdrawal model signal only deployment starts ..." >> ${log_dir}log_deploy.txt

prod_dir=/code/model_scoring/
prod_support_dir=/code/model_scoring/withdrawal_signalonly/
model_dir=/fraud_model/Results/Model_Results_Signal_Only_v2wd_woeSmth=0_newest_time/RandomForest_signal/
impwoe_dir=/fraud_model/Data/Model_Data_Signal_Tmx_v2wd_signalonly_newest_time/

cp ${model_dir}model.p    $prod_support_dir
gzip ${prod_support_dir}model.p -f
cp ${model_dir}trivial_input_values.p    $prod_support_dir
cp ${impwoe_dir}risk_table.p    $prod_support_dir
cp ${impwoe_dir}impute_values.p    $prod_support_dir
python ${prod_dir}model_scoring_prod.py 2 >>${log_dir}log_deploy.txt  2>&1

echo "["$(date)"] Withdrawal model signal only deployment is done ..." >> ${log_dir}log_deploy.txt



################### copy files and test : payment model signal tmx ##################
echo "["$(date)"] Payment model signal tmx deployment starts ..." >> ${log_dir}log_deploy.txt

prod_dir=/code/model_scoring/
prod_support_dir=/code/model_scoring/payment_signaltmx/
model_dir=/fraud_model/Results/Model_Results_Signal_Tmx_v3pmt_woeSmth=0_newest_time/RandomForest_signal_rc_tmxrc_ind/
impwoe_dir=/fraud_model/Data/Model_Data_Signal_Tmx_v3pmt_newest_time/

cp ${model_dir}model.p    $prod_support_dir
gzip ${prod_support_dir}model.p -f
cp ${model_dir}trivial_input_values.p    $prod_support_dir
cp ${impwoe_dir}risk_table.p    $prod_support_dir
cp ${impwoe_dir}impute_values.p    $prod_support_dir
python ${prod_dir}model_scoring_prod.py 3 >>${log_dir}log_deploy.txt  2>&1

echo "["$(date)"] Payment model signal tmx deployment is done ..." >> ${log_dir}log_deploy.txt



################### copy files and test : withdrawal model signal tmx ##################
echo "["$(date)"] Withdrawal model signal tmx deployment starts ..." >> ${log_dir}log_deploy.txt

prod_dir=/code/model_scoring/
prod_support_dir=/code/model_scoring/withdrawal_signaltmx/
model_dir=/fraud_model/Results/Model_Results_Signal_Tmx_v3wd_woeSmth=0_newest_time/RandomForest_signal_rc_tmxrc_ind/
impwoe_dir=/fraud_model/Data/Model_Data_Signal_Tmx_v3wd_newest_time/

cp ${model_dir}model.p    $prod_support_dir
gzip ${prod_support_dir}model.p -f
cp ${model_dir}trivial_input_values.p    $prod_support_dir
cp ${impwoe_dir}risk_table.p    $prod_support_dir
cp ${impwoe_dir}impute_values.p    $prod_support_dir
python ${prod_dir}model_scoring_prod.py 4 >>${log_dir}log_deploy.txt  2>&1

echo "["$(date)"] Withdrawal model signal tmx deployment is done ..." >> ${log_dir}log_deploy.txt



################### git deploy and send success email ##################
echo "["$(date)"] Committing and pushing to GitHub ..." >> ${log_dir}log_deploy.txt

#git pull origin master --rebase
#git checkout master
#git commit -a -m "Models have been refreshed and deployed on ["$(date)"]"
#git push origin master


title="["$(date)"] Models were retrained, Performance test passed, and Successfully deployed"
echo -e "Models were retrained, Performance test passed. \n\nPerformance test summary:" >${log_dir}final_success_message.txt
echo -e $performance_test_msg >>${log_dir}final_success_message.txt
echo -e "\nModels are successfully pushed to GitHub, please closely monitor the model performance." >>${log_dir}final_success_message.txt
mutt -s "$title" -a "$perf_result_pmt_signal" -a "$perf_result_wd_signal" -a "$perf_result_pmt_signaltmx" -a "$perf_result_wd_signaltmx" -- junhe@wepay.com < ${log_dir}final_success_message.txt

echo "["$(date)"] Pushing to GitHub done, models are successfully deployed !" >> ${log_dir}log_deploy.txt
