source ~/venv/bin/activate

code_dir=/fraud_model/Code/
log_dir=/fraud_model/Code/

echo "["$(date)"] Cron job model retrain starts ..." >${log_dir}log.txt
echo "["$(date)"] Cron job model retrain starts ..." >${log_dir}log_prog.txt
echo "["$(date)"] Cron job model performance test starts ..." >${log_dir}log_deploy.txt

bash ${code_dir}2_model_retrain.sh
bash ${code_dir}3_model_deploy.sh

deactivate