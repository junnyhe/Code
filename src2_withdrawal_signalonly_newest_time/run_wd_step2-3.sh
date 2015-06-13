set -e

opt=2 # opt=1 train model; opt=2 copy model files and test

if [ $opt == 1 ]; then
	# opt=1 train model
	
	code_dir=/fraud_model/Code/src2_withdrawal_signalonly_newest_time/
	pmt_data_dir=/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt_signalonly_newest_time/
	wd_data_dir=/fraud_model/Data/Model_Data_Signal_Tmx_v2wd_signalonly_newest_time/
	result_dir=/fraud_model/Results/Model_Results_Signal_Only_v2wd_woeSmth=0_newest_time/

	python ${code_dir}step2_model_data_prep_signal_tmx/step2b_model_data_split_downsample.py $pmt_data_dir $wd_data_dir
	python ${code_dir}step2_model_data_prep_signal_tmx/step2d_model_data_impute_woe.py $wd_data_dir
	python ${code_dir}step3_model_train/step3_model_train_signal_tmx_v2.py $wd_data_dir $result_dir

elif [ $opt == 2 ]; then
	#opt=2 copy model files and test
	
	prod_dir=/code/model_scoring/
	prod_support_dir=/code/model_scoring/withdrawal_signalonly/
	model_dir=/fraud_model/Results/Model_Results_Signal_Only_v2wd_woeSmth=0_newest_time/RandomForest_signal/
	impwoe_dir=/fraud_model/Data/Model_Data_Signal_Tmx_v2wd_signalonly_newest_time/

	cp ${model_dir}model.p    $prod_support_dir
	gzip ${prod_support_dir}model.p -f
	cp ${model_dir}trivial_input_values.p    $prod_support_dir
	cp ${impwoe_dir}risk_table.p    $prod_support_dir
	cp ${impwoe_dir}impute_values.p    $prod_support_dir
	cd $prod_dir
	python model_scoring_prod.py 2

fi