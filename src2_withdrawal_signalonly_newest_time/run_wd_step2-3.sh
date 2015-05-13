set -e
pmt_data_dir=/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt_signalonly_newest_time/
wd_data_dir=/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v2wd_signalonly_newest_time/
result_dir=/home/junhe/fraud_model/Results/Model_Results_Signal_Only_v2wd_woeSmth=0_newest_time/

#python step2_model_data_prep_signal_tmx/step2b_model_data_split_downsample.py $pmt_data_dir $wd_data_dir
#python step2_model_data_prep_signal_tmx/step2d_model_data_impute_woe.py $wd_data_dir
#python step3_model_train/step3_model_train_signal_tmx_v2.py $wd_data_dir $result_dir

prod_dir=/code/model_scoring/
prod_support_dir=/code/model_scoring/withdrawal_signalonly/
model_dir=/home/junhe/fraud_model/Results/Model_Results_Signal_Only_v2wd_woeSmth=0_newest_time/RandomForest_signal/
impwoe_dir=/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v2wd_signalonly_newest_time/

cp ${model_dir}model.p    $prod_support_dir
cp ${impwoe_dir}risk_table.p    $prod_support_dir
cp ${impwoe_dir}impute_values.p    $prod_support_dir
cd $prod_dir
python model_scoring_prod.py 2