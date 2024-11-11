#!/bin/bash

# NAME: daily_invoice_count_extract.sh
# DATE: 15 Mar 2023
# DESCRIPTION: This script is used to check completion of Cbi_procedure and error, then check count between VPRD and Redshift
# ============================================================================================================================
# DATE         MODIFIED BY            DESCRIPTION
# 15/03/2023  Siddharth Thorat         Created
# 22/03/2023  Siddharth Thorat		   RunDate change and main_steps_trigger time added in script
# 04/05/2023  Siddharth Thorat         Error checking logic added for cbi_proceducer
# 09/05/2023  Siddharth Thorat         Sending message on MS-Teams added 
# 15/05/2023  Siddharth Thorat		   main_steps name is changed to vprd_fivetran_sync_steps and main_step_runtime to vprd_fivetran_sync_runtime
# ==============================================================================================================================


d=`date +%Y%m%d%H%M%S`
run_date=`date +%Y%m%d`
#run_date='20230315'
prev_day=`date +%Y-%m-%d -d yesterday`
echo $d
main_path=/home/oracle/daily_invoice_count_check/
log_dir=/home/oracle/daily_invoice_count_check/log
s3path=s3://desototech/sid/richter_invoice
script_path_vprd=/home/oracle/daily_invoice_count_check/scripts_files_vprd
s3path_rs=s3://desototech/sid/Redshift_invoice
script_path_rs=/home/oracle/daily_invoice_count_check/scripts_files_redshift
daily_files=/home/oracle/daily_invoice_count_check/daily_files
trigger_dir=/home/oracle/daily_invoice_count_check/trigger_files
trigger_s3_counts=s3://desototech/DWH_team/sid/Daily_invoice_trigger/invoice_count_trigger/
trigger_s3_cbi=s3://desototech/Daily_invoice_trigger/cbi_procedures_trigger/
#trigger_s3_cbi=s3://desototech/DWH_team/sid/Daily_invoice_trigger/cbi_procedures_trigger/

start_time=`date '+%F %T'`

vprd_fivetran_sync_runtime=0015

echo "start time : " `date '+%F %T'`

echo "run date : " ${run_date}
echo "prev date : " ${prev_day}

recipients="mprajapati@desototechnologies.com,dgoswami@desototechnologies.com,msshah@desototechnologies.com,atopre@desototechnologies.com,apadhiar@desototechnologies.com,kparate@desototechnologies.com,sthorat@desototechnologies.com"

error_recipients="mprajapati@desototechnologies.com,dgoswami@desototechnologies.com,msshah@desototechnologies.com,atopre@desototechnologies.com,apadhiar@desototechnologies.com,kparate@desototechnologies.com,sthorat@desototechnologies.com,toneil@careismatic.com"

#recipients="sthorat@desototechnologies.com"

#cd ${main_path}
vprd_fivetran_sync_steps(){
	echo "************************************************************"
	echo "function  called - vprd_fivetran_sync_steps"
	
	run_dt=`date +%Y%m%d`
	prev_day=`date +%Y-%m-%d -d yesterday`
	
	echo "run date : " ${run_dt}
	echo "prev date : " ${prev_day}
	
	cd ${main_path}
	
	python3 vprd_generic_ext.py -d ${run_dt} -c extorclist.config -f /home/oracle/daily_invoice_count_check/scripts_files_vprd/sql_manual_extract_vprd.lst > ${log_dir}/log_vprd_$run_dt.log &
	sleep 5s
	
	python3 redshift_generic_ext.py -d ${run_dt} -c redshift_conn.config -f /home/oracle/daily_invoice_count_check/scripts_files_redshift/sql_manual_extract_redshift.lst > ${log_dir}/log_redshift_$run_dt.log &
	sleep 5s
	
	while true
	do
	s1=`grep complete ${log_dir}/log_vprd_${run_dt}.log |  awk '{ print $3 }'`
	s2=`grep complete ${log_dir}/log_redshift_${run_dt}.log |  awk '{ print $3 }'`
	
	if [[ ${s1} == 'complete' && ${s2} == 'complete' ]]; then
	        echo "extaction completed Successfully"
	        echo 'completion time : ' `date '+%F %T'`
	        break
	else
	        sleep 10
	fi
	done
	
	#Checking difference between VPRD and Redshift,store it to file
	cd ${daily_files}/${run_dt}/
	diff -B max_invoice_number_and_date_vprd.csv max_invoice_number_and_date_redshift.csv > diff_invoice_${run_dt}.txt
	
	echo "extract total size of the file and store into variable"
	#file_size_diff_file=$(ls -l diff_invoice_${run_dt}.txt | awk '{print $5}')
	
	file_size=$(ls -l diff_invoice_${run_dt}.txt | awk '{print $5}')
	echo "file size is : "${file_size}
	if [ "${file_size}" -ne "0" ]; then
			echo "count mismatched" > ${log_dir}/wrapper_${run_dt}.log
			value=$(<diff_invoice_${run_dt}.txt)
			echo "$value"
			
			Source_Counts=$(<max_invoice_number_and_date_vprd.csv)
			Destination_Counts=$(<max_invoice_number_and_date_redshift.csv)
			
	
/usr/sbin/sendmail ${recipients} <<MAIL_END
To: ${recipients}
Subject: Alert! Daily Invoice mismatch found on $run_dt

There is mismatch between vprd and Redshift invoice count.

Header : max_invoice_num|max(invo_created_date)|max(invo_mod_date)

VPRD  : ${Source_Counts}
Redshift : ${Destination_Counts}


Start time : ${start_time}
Completion time : `date '+%F %T'`
MAIL_END
	
	else
		echo "count matched" > ${log_dir}/wrapper_${run_dt}.log
	
	fi
	
	if grep "count matched" ${log_dir}/wrapper_${run_dt}.log;then
			echo "Richter and Redshift invoice count matched"
			cd /home/oracle/daily_invoice_count_check
			python3 vprd_generic_ext.py -d ${run_dt} -c extorclist.config -f /home/oracle/daily_invoice_count_check/scripts_files_vprd/sql_vprd_invoice_count.lst > ${log_dir}/vprd_count_$run_dt.log &
			sleep 30s
			
			python3 redshift_generic_ext.py -d ${run_dt} -c redshift_conn.config -f /home/oracle/daily_invoice_count_check/scripts_files_redshift/sql_redshift_invoice_count.lst > ${log_dir}/redshift_count_$run_dt.log &
			sleep 30s
			
			cd ${daily_files}/${run_dt}/
			vprd_Counts=$(<vprd_invoice_count.csv)
			Redshift_Counts=$(<redshift_count.csv)
			Source_Counts=$(<max_invoice_number_and_date_vprd.csv)
			Destination_Counts=$(<max_invoice_number_and_date_redshift.csv)
		
		echo "vprd_count: " "$vprd_Counts"
		echo "Redshift_count: " "$Redshift_Counts"
		echo "Source_Counts: " "$Source_Counts"
		echo "Destination_Counts: " "$Destination_Counts"
	
/usr/sbin/sendmail ${recipients} <<MAIL_END
To: ${recipients}
Subject: Daily Invoice count matched on  $run_dt


Today's max invoice count and time have been matched for both source and destination.

Header : max_invoice_num|max(invo_created_date)|max(invo_mod_date)

VPRD  : ${Source_Counts}
Redshift : ${Destination_Counts}

Please find below total number of invoices to be processed today:

VPRD_invoice_count : ${vprd_Counts}
Redshift_invoice_count : ${Redshift_Counts}

Start time : ${start_time}
Completion time : `date '+%F %T'`
MAIL_END
	
	fi
	
#calling checking_count_matched
checking_count_matched ${Redshift_Counts}


}

checking_count_matched(){
	echo "function  called - checking_count_matched"
	
	invoice_counts=$1
	
	if grep "count matched" ${log_dir}/wrapper_${run_dt}.log;then
		echo "There is no mismatch found between VPRD and Redshift invoice counts"
		touch ${trigger_dir}/invoice_counts_matched_trigger_${run_dt}.txt
		sleep 10s
		/usr/local/bin/aws s3 cp ${trigger_dir}/invoice_counts_matched_trigger_${run_dt}.txt ${trigger_s3_counts}
		echo "invoice count matched trigger file is copied successfully on S3"
		#sending message on teams
		teams_message ${invoice_counts}
		exit
	else
		#calling wait_for_count_matched function for 5 min sleep
		echo "waiting for count matched"
		wait_for_count_matched
	fi
}


teams_message(){
	echo "************************************************************"
	echo "function  called - teams_message"
	echo "************************************************************"
	
	message_1="Invoice count matched between Source(VPRD) and Fivetran."
	message_2="Total "$1" invoices today for ETL delta loads"
	
	#echo $1 $2 $3 
	echo ${message_2}
	
	curl -H 'Content-Type: application/json' -d '{"text": "'"$message_1"'"}' https://o365spi.webhook.office.com/webhookb2/769fb003-054d-4161-8e90-54b0e5f48576@bdeeee28-22ab-472f-8510-87812e5557e1/IncomingWebhook/f1418fc8d5ed4b6695279ad3c2bf5095/329a3b08-1316-452c-a496-6ab47e68127d
	
	sleep 5s
	
	curl -H 'Content-Type: application/json' -d '{"text": "'"$message_2"'"}' https://o365spi.webhook.office.com/webhookb2/769fb003-054d-4161-8e90-54b0e5f48576@bdeeee28-22ab-472f-8510-87812e5557e1/IncomingWebhook/f1418fc8d5ed4b6695279ad3c2bf5095/329a3b08-1316-452c-a496-6ab47e68127d
	
	sleep 5s
	
	#calling checking count function to see counts are matched or not, if it's matched then create trigger file on S3
	echo "#####################################################################################################################"
}


#sleep for 5 minutes
wait_for_count_matched(){
	echo "************************************************************"
	echo "going to sleep for 5 minutes"
	echo "************************************************************"
	sleep 300s
	
	#calling vprd_fivetran_sync_steps to verify invoice count again between VPRD and Redshift
	echo "#####################################################################################################################"
	vprd_fivetran_sync_steps
}


cbi_procedures(){
	echo "************************************************************"
	
	cd ${main_path}
	
	python3 vprd_generic_ext.py -d ${run_date} -c extorclist.config -f /home/oracle/daily_invoice_count_check/scripts_files_vprd/sql_cbi_procedures_message.lst > ${log_dir}/cbi_procedures_$run_date.log &
	sleep 5s
	
	while true
	do
	s3=`grep complete ${log_dir}/cbi_procedures_${run_date}.log |  awk '{ print $3 }'`
	
	if [[ ${s3} == 'complete' ]]; then
	        echo "python for cbi_procedures completed Successfully"
	        echo 'completion time : ' `date '+%F %T'`
	        break
	else
	        sleep 10
	fi
	done
	
	
	if grep -i 'ok' ${daily_files}/${run_date}/cbi_procedures_message.csv;then
		echo "CBI PROCEDURES is completed"
		touch ${trigger_dir}/cbi_procedures_trigger_${run_date}.txt
		sleep 10s
		/usr/local/bin/aws s3 cp ${trigger_dir}/cbi_procedures_trigger_${run_date}.txt ${trigger_s3_cbi}
		echo "cbi_procedures trigger file is copied successfully on S3"
		echo "************************************************************"
		echo "calling credit_memo_and_invoices_count function to check Today's Creadit Memo and Invoice counts"
		credit_memo_and_invoices_count
		exit
	else
		echo "CBI PROCEDURE is not completed"

/usr/sbin/sendmail ${recipients} <<MAIL_END
To: ${recipients}
Subject: Alert! CBI PROCEDURE is not completed on $run_date

CBI PROCEDURE is not completed yet for SalesJournal.

Completion time : `date '+%F %T'`
MAIL_END
	
	fi
	error_check
	#cbi_sleep
}

#Checking Error for CBI_procedures 
error_check(){
	echo "************************************************************"
	echo "function  called - error_check"
	echo "************************************************************"
	
	cd ${main_path}
	
	python3 vprd_generic_ext.py -d ${run_date} -c extorclist.config -f /home/oracle/daily_invoice_count_check/scripts_files_vprd/sql_error_check.lst > ${log_dir}/error_check_$run_date.log &
	sleep 5s
	
	while true
	do
	s4=`grep complete ${log_dir}/error_check_${run_date}.log |  awk '{ print $3 }'`
	
	if [[ ${s4} == 'complete' ]]; then
	        echo "Python for Error check completed Successfully"
	        echo 'completion time : ' `date '+%F %T'`
	        break
	else
	        sleep 10
	fi
	done
	
	cd ${daily_files}/${run_date}/
	
	#Checking file size of error file
	file_size=$(ls -l error_check.csv | awk '{print $5}')
	echo "$file_size"
	
	if [[ "${file_size}" == "0" ]]; then
		echo "There is no any error found"
	else
		echo "Hey.!!! Error found !!"
		#value=$(<error_check.csv)
		#echo "$value"
		
		echo "Sending error notification on teams"
		
		error_message="Alert! Error found in CBI_procedures for invoice generation process. Kindly check your email for the same"
		
		curl -H 'Content-Type: application/json' -d '{"text": "'"$error_message"'"}' https://o365spi.webhook.office.com/webhookb2/769fb003-054d-4161-8e90-54b0e5f48576@bdeeee28-22ab-472f-8510-87812e5557e1/IncomingWebhook/f1418fc8d5ed4b6695279ad3c2bf5095/329a3b08-1316-452c-a496-6ab47e68127d
		
		sleep 2s
		
		echo "Sending mail notification for error"
		echo -e "There is an error found while processing CBI_procedure for SalesJournal:" | mailx -s "Alert! Error found for CBI_procedure SalesJournal on $run_date" -a ${daily_files}/${run_date}/error_check.csv ${error_recipients}
	fi
	
	#calling cbi_sleep to wait till CBI_procedure is completed
	echo "#####################################################################################################################"
	cbi_sleep
}

#This function check invoice and Creadit Memo count
credit_memo_and_invoices_count(){
	echo "************************************************************"
	
	cd ${main_path}
	
	python3 vprd_generic_ext.py -d ${run_date} -c extorclist.config -f /home/oracle/daily_invoice_count_check/scripts_files_vprd/sql_CM_and_IN_invoice_counts.lst > ${log_dir}/CM_and_IN_invoice_counts_$run_date.log &
	sleep 5s
	
	while true
	do
	s5=`grep complete ${log_dir}/CM_and_IN_invoice_counts_${run_date}.log |  awk '{ print $3 }'`
	
	if [[ ${s5} == 'complete' ]]; then
	        echo "python for CM_and_IN_invoice_counts completed Successfully"
	        echo 'completion time : ' `date '+%F %T'`
	        break
	else
	        sleep 10
	fi
	done
	
	
	cd ${daily_files}/${run_date}/
	
	echo "Checking for Credit Memo and Invoice count"
	
	#changing '|' to '-' using sed command
	sed -i 's/|/-/g' CM_and_IN_invoice_counts.csv
	sleep 5s
	
	#cm_in_counts=$(<CM_and_IN_invoice_counts.csv)
	
	#Storing invoice and Credit Memo counts in variable 
	invoice_counts=`grep -i invoice CM_and_IN_invoice_counts.csv | awk '{print $1}'`
	CM_counts=`grep -i credit_memo CM_and_IN_invoice_counts.csv | awk '{print $1}'`
	
	final_message="${invoice_counts} and ${CM_counts} in VPRD for ${run_date}"
	echo $final_message
	
	echo "Sending message in Teams for CM and IN counts"
	
	curl -H 'Content-Type: application/json' -d '{"text": "'"$final_message"'"}' https://o365spi.webhook.office.com/webhookb2/769fb003-054d-4161-8e90-54b0e5f48576@bdeeee28-22ab-472f-8510-87812e5557e1/IncomingWebhook/f1418fc8d5ed4b6695279ad3c2bf5095/329a3b08-1316-452c-a496-6ab47e68127d
	
	sleep 2s

	
	#calling check_run_date function
	echo "calling check_run_date function to check whether run_date is changed or not"
	check_run_date
}


#Checking for current date
check_run_date(){
	echo "************************************************************"
	echo "function  called - check_run_date"
	today=$(date +%Y%m%d)
	#today='20230315'
	echo "run_date is : " ${run_date}
	echo "current_date is : " ${today}
	
	if [[ "$run_date" -ne "$today" ]];then
		echo "************************************************************"
		echo "Date is changed - calling check_time function to verfiy vprd_fivetran_sync_steps run time is excceded 0010 or not"
		check_time
	else
		#calling download_log_s3 to download parent_log file again
		echo "************************************************************"      
		echo "calling function - run_date_check_sleep"        
		run_date_check_sleep
	fi
}

#run date checking sleep, it will go in loop until current date is not changed
run_date_check_sleep(){
	echo "************************************************************"
	echo "going to sleep for 5 minutes"
	echo "************************************************************"
	sleep 300s
	
	#calling check_run_date function to check current date
	echo "#####################################################################################################################"
	check_run_date
}

check_time(){
	echo "************************************************************"
	echo "function  called - check_time"
	
	current_time=`date +%H%M`
	echo "current time is :" ${current_time}
	echo "vprd_fivetran_sync_runtime is :" ${vprd_fivetran_sync_runtime}
	
	if [[ "$vprd_fivetran_sync_runtime" -ge "$current_time" ]];then
		echo "************************************************************"
		echo "vprd_fivetran_sync_runtime is not matched yet.Hence,going to sleep"
		check_time_sleep
	else
		#calling download_log_s3 to download parent_log file again
		echo "The current_time has exceeded the vprd_fivetran_sync_runtime"
		echo "************************************************************"    
		echo "calling function - vprd_fivetran_sync_steps"        
		vprd_fivetran_sync_steps
	fi
}

#cbi_proceducer sleep for 10 min
cbi_sleep(){
	echo "************************************************************"
	echo "going to sleep for 10 minutes"
	echo "************************************************************"
	sleep 600s
	
	#calling cbi_procedures to verify "OK" message
	echo "#####################################################################################################################"
	cbi_procedures
}


check_time_sleep(){
	echo "************************************************************"
	echo "going to sleep for 5 minutes"
	echo "************************************************************"
	sleep 300s
	
	#calling check_time function
	echo "#####################################################################################################################"
	check_time
}

#Invoke your function
echo "function  called - cbi_procedures"
cbi_procedures
