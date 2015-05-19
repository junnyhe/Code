set -e
data_dir=/fraud_model/Data/Model_Data_Signal_Tmx_v3pmt_newest_time/
result_dir=/fraud_model/Results/Model_Results_Signal_Tmx_v3pmt_woeSmth=0_newest_time/

year=2015
month=3
day=31

opt=2 # opt=1 train model; opt=2 copy model files and test

if [ $opt == 1 ]; then
	# opt=1 train model;
		
	python step2_model_data_preprocessing/step2a_data_concat_daily_files.py $year $month $day
	python step2_model_data_preprocessing/step2b_model_data_split_downsample.py $data_dir
	python step3_model_data_prep_signal_tmx/step3a_model_data_rc_tmxrc_ind_creation_main.py $data_dir
	python step3_model_data_prep_signal_tmx/step3b_model_data_feature_creation_main.py $data_dir
	python step3_model_data_prep_signal_tmx/step3c_support_impute_woe_mapping_calculation.py $data_dir
	python step3_model_data_prep_signal_tmx/step3c_model_data_impute_woe_assigin_main.py $data_dir
	
	python step4_model_train/step4_model_train_signal_tmx_v2.py $data_dir $result_dir

elif [ $opt == 2 ]; then
	#opt=2 copy model files and test
	
	prod_dir=/code/model_scoring/
	prod_support_dir=/code/model_scoring/payment_signaltmx/
	model_dir=/fraud_model/Results/Model_Results_Signal_Tmx_v3pmt_woeSmth=0_newest_time/RandomForest_signal_rc_tmx_rc_ind/
	impwoe_dir=/fraud_model/Data/Model_Data_Signal_Tmx_v3pmt_newest_time/
	
	cp ${model_dir}model.p    $prod_support_dir
	gzip ${prod_support_dir}model.p -f
	cp ${model_dir}trivial_input_values.p    $prod_support_dir
	cp ${impwoe_dir}risk_table.p    $prod_support_dir
	cp ${impwoe_dir}impute_values.p    $prod_support_dir
	cd $prod_dir
	python model_scoring_prod.py 3
	
fi


