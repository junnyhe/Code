year=2015
month=4
day=1
nDays=30

code_dir=/fraud_model/Code/ETL/step1_data_rollup_merge/
log_dir=/fraud_model/Code/

echo "["$(date)"] Rolling up signals starts ..." >> ${log_dir}log_prog.txt
echo "["$(date)"] Rolling up signals starts ..." >> ${log_dir}log.txt
python ${code_dir}step1a_signal_rollup.py $year $month $day $nDays >> ${log_dir}log.txt  2>&1
echo "["$(date)"] Rolling up signals done ..." >> ${log_dir}log_prog.txt
echo "["$(date)"] Rolling up signals done ..." >> ${log_dir}log.txt


echo "["$(date)"] Rolling up tmx payer starts ..." >> ${log_dir}log_prog.txt
echo "["$(date)"] Rolling up tmx payer starts ..." >> ${log_dir}log.txt
python ${code_dir}step1b_threatmetrix_payer_rollup.py $year $month $day $nDays >> ${log_dir}log.txt  2>&1
echo "["$(date)"] Rolling up tmx payer done ..." >> ${log_dir}log_prog.txt
echo "["$(date)"] Rolling up tmx payer done ..." >> ${log_dir}log.txt


echo "["$(date)"] Rolling up tmx payee starts ..." >> ${log_dir}log_prog.txt
echo "["$(date)"] Rolling up tmx payee starts ..." >> ${log_dir}log.txt
python ${code_dir}step1c_threatmetrix_payee_rollup.py $year $month $day $nDays >> ${log_dir}log.txt  2>&1
echo "["$(date)"] Rolling up tmx payee done ..." >> ${log_dir}log_prog.txt
echo "["$(date)"] Rolling up tmx payee done ..." >> ${log_dir}log.txt


echo "["$(date)"] Merging data starts ..." >> ${log_dir}log_prog.txt
echo "["$(date)"] Merging data starts ..." >> ${log_dir}log.txt
python ${code_dir}step1d_merge_signal_threatmetrix.py $year $month $day $nDays >> ${log_dir}log.txt  2>&1
echo "["$(date)"] Merging data done ..." >> ${log_dir}log_prog.txt
echo "["$(date)"] Merging data done ..." >> ${log_dir}log.txt
