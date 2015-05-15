#!/bin/bash
set -e
pmt_data_dir=/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v3pmt_newest_time/
wd_data_dir=/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v3wd_newest_time/
result_dir=/home/junhe/fraud_model/Results/Model_Results_Signal_Tmx_v3wd_woeSmth=0_newest_time/

opt=2 # opt=1 train model; opt=2 copy model files

if [ $opt == 1 ]; then
	# opt=1 train model;
	
	python step2_model_data_preprocessing/step2b_model_data_split_downsample_wd.py $pmt_data_dir $wd_data_dir
	python step3_model_data_prep_signal_tmx/step3a_model_data_rc_tmxrc_ind_creation_main.py $wd_data_dir
	python step3_model_data_prep_signal_tmx/step3b_model_data_feature_creation_main.py $wd_data_dir
	python step3_model_data_prep_signal_tmx/step3c_support_impute_woe_mapping_calculation.py $wd_data_dir
	python step3_model_data_prep_signal_tmx/step3c_model_data_impute_woe_assigin_main.py $wd_data_dir
	
	python step4_model_train/step4_model_train_signal_tmx_v2.py $wd_data_dir $result_dir
	
elif [ $opt == 2 ]; then
	#opt=2 copy modelfiles
	
	prod_dir=/code/model_scoring/
	prod_support_dir=/code/model_scoring/withdrawal_signaltmx/
	model_dir=/home/junhe/fraud_model/Results/Model_Results_Signal_Tmx_v3wd_woeSmth=0_newest_time/RandomForest_signal_rc_tmxrc_ind/
	impwoe_dir=/home/junhe/fraud_model/Data/Model_Data_Signal_Tmx_v3wd_newest_time/
	
	cp ${model_dir}model.p    $prod_support_dir
	gzip ${prod_support_dir}model.p -f
	cp ${impwoe_dir}risk_table.p    $prod_support_dir
	cp ${impwoe_dir}impute_values.p    $prod_support_dir
	cd $prod_dir
	python model_scoring_prod.py 4
	
fi

