set -e

year=2015
month=4
day=30

opt=2 # opt=1 train model; opt=2 copy model files and test

if [ $opt == 1 ]; then
	# opt=1 train model;
	
	code_dir=/fraud_model/Code/src2_payment_signalonly_newest_time/
	data_dir=/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt_signalonly_newest_time/
	result_dir=/fraud_model/Results/Model_Results_Signal_Only_v2pmt_woeSmth=0_newest_time/

	python ${code_dir}step2_model_data_prep_signal_tmx/step2a1_data_concat_daily_files.py $year $month $day
	python ${code_dir}step2_model_data_prep_signal_tmx/step2b_model_data_split_downsample.py $data_dir
	python ${code_dir}step2_model_data_prep_signal_tmx/step2d_model_data_impute_woe.py $data_dir
	python ${code_dir}step3_model_train/step3_model_train_signal_tmx_v2.py $data_dir $result_dir

elif [ $opt == 2 ]; then
	#opt=2 copy model files and test
	
	prod_dir=/code/model_scoring/
	prod_support_dir=/code/model_scoring/payment_signalonly/
	model_dir=/fraud_model/Results/Model_Results_Signal_Only_v2pmt_woeSmth=0_newest_time/RandomForest_signal/
	impwoe_dir=/fraud_model/Data/Model_Data_Signal_Tmx_v2pmt_signalonly_newest_time/
	
	cp ${model_dir}model.p    $prod_support_dir
	gzip ${prod_support_dir}model.p -f
	cp ${model_dir}trivial_input_values.p    $prod_support_dir
	cp ${impwoe_dir}risk_table.p    $prod_support_dir
	cp ${impwoe_dir}impute_values.p    $prod_support_dir
	cd $prod_dir
	python model_scoring_prod.py 1
	
fi
