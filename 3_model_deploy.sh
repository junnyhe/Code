set -e
log_dir=/fraud_model/Code/

# last day of training period
days_shift=10
year=$(date --date="$days_shift days ago" +%Y)
month=$(date --date="$days_shift days ago" +%m)
day=$(date --date="$days_shift days ago" +%d)

perf_result_pmt_signal=/fraud_model/Results/Model_Results_Signal_Only_v2pmt_woeSmth=0_newest_time/pmt_signal/performance_pmt_signal_1mo.csv
perf_result_wd_signal=/fraud_model/Results/Model_Results_Signal_Only_v2wd_woeSmth=0_newest_time/wd_signal/performance_wd_signal_1mo.csv
perf_result_pmt_signaltmx=/fraud_model/Results/Model_Results_Signal_Tmx_v3pmt_woeSmth=0_newest_time/pmt_signaltmx/performance_pmt_signaltmx_1mo.csv
perf_result_wd_signaltmx=/fraud_model/Results/Model_Results_Signal_Tmx_v3wd_woeSmth=0_newest_time/wd_signaltmx/performance_wd_signaltmx_1mo.csv

################### check model performance and send email #######################
# get precision at 70% recall
prec_at_recall_70_pmt_signal=$(sed '24q;d' $perf_result_pmt_signal | cut -d, -f9 | tr -d '\n' | tr -d '\r')
prec_at_recall_60_wd_signal=$(sed '22q;d' $perf_result_wd_signal | cut -d, -f9 | tr -d '\n' | tr -d '\r')
prec_at_recall_70_pmt_signaltmx=$(sed '24q;d' $perf_result_pmt_signaltmx | cut -d, -f9 | tr -d '\n' | tr -d '\r')
prec_at_recall_60_wd_signaltmx=$(sed '22q;d' $perf_result_wd_signaltmx | cut -d, -f9 | tr -d '\n' | tr -d '\r')


# prepare test pass/fail email message to send, and copy files when only test passed
performance_test_msg=""


if [ $(bc <<< "$prec_at_recall_70_pmt_signal > 0.18") -eq 1 ]; then
	performance_test_msg=${performance_test_msg}"pmt signal model performance test (prec_at_recall_70>0.18) PASSED: "$prec_at_recall_70_pmt_signal
	################### copy files and test : payment model signal only ##################
	echo "["$(date)"] Payment model signal only copying starts ..." >> ${log_dir}log_deploy.txt
	
	prod_dir=/fraud_model_prod/model_scoring/
	dev_support_dir=/fraud_model/Code/src2_payment_signalonly_newest_time/support_files/
	prod_support_dir=/fraud_model_prod/model_scoring/payment_signalonly/
	model_dir=/fraud_model/Results/Model_Results_Signal_Only_v2pmt_woeSmth=0_newest_time/pmt_signal/
	impwoe_dir=/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt_signalonly_newest_time/
	
	cp ${dev_support_dir}*    $prod_support_dir
	cp ${model_dir}model.p    $prod_support_dir
	gzip ${prod_support_dir}model.p -f
	cp ${model_dir}trivial_input_values.p    $prod_support_dir
	cp ${impwoe_dir}risk_table.p    $prod_support_dir
	cp ${impwoe_dir}impute_values.p    $prod_support_dir
	python ${prod_dir}model_scoring_prod.py 1 >>${log_dir}log_deploy.txt  2>&1
	
	echo "["$(date)"] Payment model signal only copying is done ..." >> ${log_dir}log_deploy.txt
else
	performance_test_msg=${performance_test_msg}"pmt signal model performance test (prec_at_recall_70>0.18) FAILED: "$prec_at_recall_70_pmt_signal
	echo -e "\n\npmt signal model performance test (prec_at_recall_70>0.18) FAILED: "${prec_at_recall_70_pmt_signal}"\n\n" >> ${log_dir}log_deploy.txt
fi


if [ $(bc <<< "$prec_at_recall_60_wd_signal > 0.08") -eq 1 ]; then
	performance_test_msg=${performance_test_msg}"\nwd signal model performance test (prec_at_recall_60>0.08) PASSED: "$prec_at_recall_60_wd_signal
	################### copy files and test : withdrawal model signal only ##################
	echo "["$(date)"] Withdrawal model signal only copying starts ..." >> ${log_dir}log_deploy.txt
	
	prod_dir=/fraud_model_prod/model_scoring/
	dev_support_dir=/fraud_model/Code/src2_withdrawal_signalonly_newest_time/support_files/
	prod_support_dir=/fraud_model_prod/model_scoring/withdrawal_signalonly/
	model_dir=/fraud_model/Results/Model_Results_Signal_Only_v2wd_woeSmth=0_newest_time/wd_signal/
	impwoe_dir=/fraud_model/Data/Model_Data_Signal_Tmx_v2wd_signalonly_newest_time/
	
	cp ${dev_support_dir}*    $prod_support_dir
	cp ${model_dir}model.p    $prod_support_dir
	gzip ${prod_support_dir}model.p -f
	cp ${model_dir}trivial_input_values.p    $prod_support_dir
	cp ${impwoe_dir}risk_table.p    $prod_support_dir
	cp ${impwoe_dir}impute_values.p    $prod_support_dir
	python ${prod_dir}model_scoring_prod.py 2 >>${log_dir}log_deploy.txt  2>&1
	
	echo "["$(date)"] Withdrawal model signal only copying is done ..." >> ${log_dir}log_deploy.txt
else
	performance_test_msg=${performance_test_msg}"\nwd signal model performance test (prec_at_recall_60>0.08) FAILED: "$prec_at_recall_60_wd_signal
	echo -e "\n\nwd signal model performance test (prec_at_recall_60>0.08) FAILED: "${prec_at_recall_60_wd_signal}"\n\n" >> ${log_dir}log_deploy.txt
fi


if [ $(bc <<< "$prec_at_recall_70_pmt_signaltmx > 0.18") -eq 1 ]; then
	performance_test_msg=${performance_test_msg}"\npmt signaltmx model performance test (prec_at_recall_70>0.18) PASSED: "$prec_at_recall_70_pmt_signaltmx
	################### copy files and test : payment model signal tmx ##################
	echo "["$(date)"] Payment model signal tmx copying starts ..." >> ${log_dir}log_deploy.txt
	
	prod_dir=/fraud_model_prod/model_scoring/
	dev_support_dir=/fraud_model/Code/src3_payment_signaltmx_newest_time/support_files/
	prod_support_dir=/fraud_model_prod/model_scoring/payment_signaltmx/
	model_dir=/fraud_model/Results/Model_Results_Signal_Tmx_v3pmt_woeSmth=0_newest_time/pmt_signaltmx/
	impwoe_dir=/fraud_model/Data/Model_Data_Signal_Tmx_v3pmt_newest_time/
	
	cp ${dev_support_dir}*    $prod_support_dir
	cp ${model_dir}model.p    $prod_support_dir
	gzip ${prod_support_dir}model.p -f
	cp ${model_dir}trivial_input_values.p    $prod_support_dir
	cp ${impwoe_dir}risk_table.p    $prod_support_dir
	cp ${impwoe_dir}impute_values.p    $prod_support_dir
	python ${prod_dir}model_scoring_prod.py 3 >>${log_dir}log_deploy.txt  2>&1
	
	echo "["$(date)"] Payment model signal tmx copying is done ..." >> ${log_dir}log_deploy.txt	
else
	performance_test_msg=${performance_test_msg}"\npmt signaltmx model performance test (prec_at_recall_70>0.18) FAILED: "$prec_at_recall_70_pmt_signaltmx
	echo -e "\n\npmt signaltmx model performance test (prec_at_recall_70>0.18) FAILED: "${prec_at_recall_70_pmt_signaltmx}"\n\n" >> ${log_dir}log_deploy.txt
fi


if [ $(bc <<< "$prec_at_recall_60_wd_signaltmx > 0.08") -eq 1 ]; then
	performance_test_msg=${performance_test_msg}"\nwd signaltmx model performance test (prec_at_recall_60>0.08) PASSED: "$prec_at_recall_60_wd_signaltmx
	################### copy files and test : withdrawal model signal tmx ##################
	echo "["$(date)"] Withdrawal model signal tmx copying starts ..." >> ${log_dir}log_deploy.txt
	
	prod_dir=/fraud_model_prod/model_scoring/
	dev_support_dir=/fraud_model/Code/src3_withdrawal_signaltmx_newest_time/support_files/
	prod_support_dir=/fraud_model_prod/model_scoring/withdrawal_signaltmx/
	model_dir=/fraud_model/Results/Model_Results_Signal_Tmx_v3wd_woeSmth=0_newest_time/wd_signaltmx/
	impwoe_dir=/fraud_model/Data/Model_Data_Signal_Tmx_v3wd_newest_time/
	
	cp ${dev_support_dir}*    $prod_support_dir
	cp ${model_dir}model.p    $prod_support_dir
	gzip ${prod_support_dir}model.p -f
	cp ${model_dir}trivial_input_values.p    $prod_support_dir
	cp ${impwoe_dir}risk_table.p    $prod_support_dir
	cp ${impwoe_dir}impute_values.p    $prod_support_dir
	python ${prod_dir}model_scoring_prod.py 4 >>${log_dir}log_deploy.txt  2>&1
	
	echo "["$(date)"] Withdrawal model signal tmx copying is done ..." >> ${log_dir}log_deploy.txt
else
	performance_test_msg=${performance_test_msg}"\nwd signaltmx model performance test (prec_at_recall_60>0.08) FAILED: "$prec_at_recall_60_wd_signaltmx
	echo -e "\n\nwd signaltmx model performance test (prec_at_recall_60>0.08) FAILED: "${prec_at_recall_60_wd_signaltmx}"\n\n" >> ${log_dir}log_deploy.txt
fi




################### git deploy and send success email ##################
echo -e $performance_test_msg>> ${log_dir}log_deploy.txt
echo "["$(date)"] Committing and pushing to GitHub ..." >> ${log_dir}log_deploy.txt 
cd /fraud_model_prod
git checkout master >>${log_dir}log_deploy.txt  2>&1
git commit -a -m "Models have been refreshed and deployed on [$(date)]" >>${log_dir}log_deploy.txt  2>&1
git pull --rebase origin master >>${log_dir}log_deploy.txt  2>&1
git push origin master >>${log_dir}log_deploy.txt  2>&1


if [[ $performance_test_msg =~ "FAIL" ]]; then
    title="[Models] One or More Models Failed Performance Tests. Models that passed were deployed. [$(date)]"
    echo "["$(date)"] Performance test failed for one or more models ! Models that passed performance tests are successfully pushed to GitHub ..." >> ${log_dir}log_deploy.txt
else
    title="[Models] All Fraud Models were Successfully refreshed and deployed [$(date)]"
	echo "["$(date)"] Pushing to GitHub done, models are successfully deployed !" >> ${log_dir}log_deploy.txt
fi

echo -e "Fraud models were retrained on a 80-day-period ending on $(date --date="$days_shift days ago" +%Y-%m-%d). Performance were tested on 30~1 days before training. \n\nPerformance test summary:" >${log_dir}final_success_message.txt
echo -e $performance_test_msg >>${log_dir}final_success_message.txt
echo -e "\nModels that passed performance tests have been successfully pushed to GitHub, please closely monitor the model performance." >>${log_dir}final_success_message.txt
mutt -s "$title" -a "$perf_result_pmt_signal" -a "$perf_result_wd_signal" -a "$perf_result_pmt_signaltmx" -a "$perf_result_wd_signaltmx" -- junhe@wepay.com,ping.li@wepay.com,john.canfield@wepay.com,shyamm@wepay.com,andre@wepay.com,yaqit@wepay.com,zoraz@wepay.com< ${log_dir}final_success_message.txt

