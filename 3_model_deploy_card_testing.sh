set -e
log_dir=/fraud_model/Code/


perf_result_pmt_signal=/fraud_model/Results/card_testing_test_collusion/pmt_signal/performance_pmt_signal_2mo.csv

################### check model performance and send email #######################
# get precision at 70% recall
prec_at_recall_70_pmt_signal=$(sed '24q;d' $perf_result_pmt_signal | cut -d, -f9 | tr -d '\n' | tr -d '\r')


# prepare test pass/fail email message to send, and copy files when only test passed
performance_test_msg=""


if [ $(bc <<< "$prec_at_recall_70_pmt_signal > 0.25") -eq 1 ]; then
	performance_test_msg=${performance_test_msg}"pmt signal model performance test (prec_at_recall_70>0.25) PASSED: "$prec_at_recall_70_pmt_signal
	################### copy files and test : payment model signal only ##################
	echo "["$(date)"] Payment model signal only copying starts ..." >> ${log_dir}log_deploy.txt
	
	prod_dir=/card_testing_model_prod_test_collusion/model_scoring/
	dev_support_dir=/fraud_model/Code/src4_card_testing_test_collusion/support_files/
	prod_support_dir=/card_testing_model_prod_test_collusion/model_scoring/card_testing/
	model_dir=/fraud_model/Results/card_testing_test_collusion/pmt_signal/
	impwoe_dir=/fraud_model/Data/card_testing_test_collusion/
	
	cp ${dev_support_dir}*    $prod_support_dir
	cp ${model_dir}model.p    $prod_support_dir
	gzip ${prod_support_dir}model.p -f
	cp ${model_dir}trivial_input_values.p    $prod_support_dir
	cp ${impwoe_dir}risk_table.p    $prod_support_dir
	cp ${impwoe_dir}impute_values.p    $prod_support_dir
	python ${prod_dir}model_scoring_prod.py 5 >>${log_dir}log_deploy.txt  2>&1
	
	echo "["$(date)"] Payment model signal only copying is done ..." >> ${log_dir}log_deploy.txt
else
	performance_test_msg=${performance_test_msg}"pmt signal model performance test (prec_at_recall_70>0.25) FAILED: "$prec_at_recall_70_pmt_signal
	echo -e "\n\npmt signal model performance test (prec_at_recall_70>0.25) FAILED: "${prec_at_recall_70_pmt_signal}"\n\n" >> ${log_dir}log_deploy.txt
fi



################### git deploy and send success email ##################


if [[ $performance_test_msg =~ "FAIL" ]]; then
    title="[Models] One or More Models Failed Performance Tests. Models that passed were deployed. [$(date)]"
    echo "["$(date)"] Performance test failed for one or more models ! Models that passed performance tests are successfully pushed to GitHub ..." >> ${log_dir}log_deploy.txt
else
    title="[Models] All Fraud Models were Successfully refreshed and deployed [$(date)]"
	echo "["$(date)"] Pushing to GitHub done, models are successfully deployed !" >> ${log_dir}log_deploy.txt
fi

echo -e "Fraud models were retrained on a 80-day-period ending on $(date --date="$days_shift days ago" +%Y-%m-%d). Performance were tested on 60~30 days before training. \n\nPerformance test summary:" >${log_dir}final_success_message.txt
echo -e $performance_test_msg >>${log_dir}final_success_message.txt
echo -e "\nModels that passed performance tests have been successfully pushed to GitHub, please closely monitor the model performance." >>${log_dir}final_success_message.txt
mutt -s "$title" -a "$perf_result_pmt_signal"  -- junhe@wepay.com< ${log_dir}final_success_message.txt

