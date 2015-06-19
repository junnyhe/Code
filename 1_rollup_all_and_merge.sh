set -e

days_shift=10 # days to shift back for the last day to rollup
year=$(date --date="$days_shift days ago" +%Y)
month=$(date --date="$days_shift days ago" +%m)
day=$(date --date="$days_shift days ago" +%d)

nDays=28  # number of days to try for rollup, if files have not been rolled up

code_dir=/fraud_model/Code/ETL/step1_data_rollup_merge/
log_dir=/fraud_model/Code/

echo "["$(date)"] Rolling up signals starts (lastDay=$year-$month-$day, nDaysBack=$nDays) ..." >> ${log_dir}log_prog.txt
echo "["$(date)"] Rolling up signals starts (lastDay=$year-$month-$day, nDaysBack=$nDays) ..." >> ${log_dir}log.txt
python ${code_dir}step1a_signal_rollup.py $year $month $day $nDays >> ${log_dir}log.txt  2>&1
echo "["$(date)"] Rolling up signals done (lastDay=$year-$month-$day, nDaysBack=$nDays) ..." >> ${log_dir}log_prog.txt
echo "["$(date)"] Rolling up signals done (lastDay=$year-$month-$day, nDaysBack=$nDays) ..." >> ${log_dir}log.txt


echo "["$(date)"] Rolling up tmx payer starts (lastDay=$year-$month-$day, nDaysBack=$nDays) ..." >> ${log_dir}log_prog.txt
echo "["$(date)"] Rolling up tmx payer starts (lastDay=$year-$month-$day, nDaysBack=$nDays) ..." >> ${log_dir}log.txt
python ${code_dir}step1b_threatmetrix_payer_rollup.py $year $month $day $nDays >> ${log_dir}log.txt  2>&1
echo "["$(date)"] Rolling up tmx payer done (lastDay=$year-$month-$day, nDaysBack=$nDays) ..." >> ${log_dir}log_prog.txt
echo "["$(date)"] Rolling up tmx payer done (lastDay=$year-$month-$day, nDaysBack=$nDays) ..." >> ${log_dir}log.txt


echo "["$(date)"] Rolling up tmx payee starts (lastDay=$year-$month-$day, nDaysBack=$nDays) ..." >> ${log_dir}log_prog.txt
echo "["$(date)"] Rolling up tmx payee starts (lastDay=$year-$month-$day, nDaysBack=$nDays) ..." >> ${log_dir}log.txt
python ${code_dir}step1c_threatmetrix_payee_rollup.py $year $month $day $nDays >> ${log_dir}log.txt  2>&1
echo "["$(date)"] Rolling up tmx payee done (lastDay=$year-$month-$day, nDaysBack=$nDays) ..." >> ${log_dir}log_prog.txt
echo "["$(date)"] Rolling up tmx payee done (lastDay=$year-$month-$day, nDaysBack=$nDays) ..." >> ${log_dir}log.txt


echo "["$(date)"] Merging data starts (lastDay=$year-$month-$day, nDaysBack=$nDays) ..." >> ${log_dir}log_prog.txt
echo "["$(date)"] Merging data starts (lastDay=$year-$month-$day, nDaysBack=$nDays) ..." >> ${log_dir}log.txt
python ${code_dir}step1d_merge_signal_threatmetrix.py $year $month $day $nDays >> ${log_dir}log.txt  2>&1
echo "["$(date)"] Merging data done (lastDay=$year-$month-$day, nDaysBack=$nDays) ..." >> ${log_dir}log_prog.txt
echo "["$(date)"] Merging data done (lastDay=$year-$month-$day, nDaysBack=$nDays) ..." >> ${log_dir}log.txt
