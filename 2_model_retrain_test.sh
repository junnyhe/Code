#sudo ln -sf /usr/share/zoneinfo/America/Los_Angeles /etc/localtime
set -e
log_dir=/fraud_model/Code/

days_shift=10
year=$(date --date="$days_shift days ago" +%Y)
month=$(date --date="$days_shift days ago" +%m)
day=$(date --date="$days_shift days ago" +%d)


 ################### payment model signal only ##################
echo "["$(date)"] Payment model signal only training starts ... (Last day trained: $year-$month-$day)" >>${log_dir}log_prog.txt
echo "["$(date)"] Payment model signal only training starts ... (Last day trained: $year-$month-$day)" >>${log_dir}log_test.txt

code_dir=/fraud_model/Code/src2_payment_signalonly_newest_time_test/
data_dir=/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt_signalonly_newest_time_test/
support_dir=/fraud_model/Code/src2_payment_signalonly_newest_time_test/support_files/
result_dir=/fraud_model/Results/Model_Results_Signal_Only_v2pmt_woeSmth=0_newest_time_test/

python ${code_dir}step2_model_data_prep_signal_tmx/step2a1_data_concat_daily_files.py $year $month $day >>${log_dir}log_test.txt  2>&1
python ${code_dir}step2_model_data_prep_signal_tmx/step2b_model_data_split_downsample.py $data_dir >>${log_dir}log_test.txt  2>&1
python ${code_dir}step2_model_data_prep_signal_tmx/step2d_model_data_impute_woe.py $data_dir $support_dir >>${log_dir}log_test.txt  2>&1
python ${code_dir}step3_model_train/step3_model_train_signal_tmx_v2.py $data_dir $support_dir $result_dir >>${log_dir}log_test.txt  2>&1

echo "["$(date)"] Payment model signal only training is done ..." >>${log_dir}log_prog.txt
echo "["$(date)"] Payment model signal only training is done ..." >>${log_dir}log_test.txt



 ################## withdrawal model signal only ##################
echo "["$(date)"] Withdrawal model signal only training starts ..." >>${log_dir}log_prog.txt
echo "["$(date)"] Withdrawal model signal only training starts ..." >>${log_dir}log_test.txt

code_dir=/fraud_model/Code/src2_withdrawal_signalonly_newest_time_test/
pmt_data_dir=/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt_signalonly_newest_time_test/
wd_data_dir=/fraud_model/Data/Model_Data_Signal_Tmx_v2wd_signalonly_newest_time_test/
support_dir=/fraud_model/Code/src2_withdrawal_signalonly_newest_time_test/support_files/
result_dir=/fraud_model/Results/Model_Results_Signal_Only_v2wd_woeSmth=0_newest_time_test/

python ${code_dir}step2_model_data_prep_signal_tmx/step2b_model_data_split_downsample.py $pmt_data_dir $wd_data_dir >>${log_dir}log_test.txt  2>&1
python ${code_dir}step2_model_data_prep_signal_tmx/step2d_model_data_impute_woe.py $wd_data_dir $support_dir >>${log_dir}log_test.txt  2>&1
python ${code_dir}step3_model_train/step3_model_train_signal_tmx_v2.py $wd_data_dir $support_dir $result_dir >>${log_dir}log_test.txt  2>&1

echo "["$(date)"] Withdrawal model signal only training is done ..." >>${log_dir}log_prog.txt
echo "["$(date)"] Withdrawal model signal only training is done ..." >>${log_dir}log_test.txt

