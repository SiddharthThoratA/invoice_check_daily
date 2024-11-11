# NAME: daily_invoice_count_extract.py
# DATE: 15 Mar 2023
# DESCRIPTION: This script is used to check completion of Cbi_procedure and error, then check count between VPRD and Redshift
# ============================================================================================================================
# DATE         MODIFIED BY            DESCRIPTION
# 15/03/2023  Siddharth Thorat         Created
# 22/03/2023  Siddharth Thorat		   RunDate change and main_steps_trigger time added in script
# 04/05/2023  Siddharth Thorat         Error checking logic added for cbi_proceducer
# 09/05/2023  Siddharth Thorat         Sending message on MS-Teams added 
# 15/05/2023  Siddharth Thorat		   main_steps name is changed to vprd_fivetran_sync_steps and main_step_runtime to vprd_fivetran_sync_runtime
# 09/05/2024  Kartik Parate            Converted shell script to python.
# ==============================================================================================================================


import os
import time
from datetime import datetime, timedelta
import subprocess
import sys, getopt
import csv
import gzip
import shutil
import re
import json
import fileinput
import ctypes

ctypes.windll.kernel32.SetConsoleTitleW("daily_invoice_count_extract.py")
d = datetime.now().strftime('%Y%m%d%H%M%S')
run_date = datetime.now().strftime('%Y%m%d')
prev_day=(datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
#run_date = "20240514"
#prev_day= "2024-05-13"
print(d)
main_path = "D:\\oracle\\daily_invoice_count_check"
log_dir = "D:\\oracle\\daily_invoice_count_check\\log"
s3path="s3://desototech/sid/richter_invoice"
script_path_vprd = "D:\\oracle\\daily_invoice_count_check\\scripts_files_vprd"
s3path_rs="s3://desototech/sid/Redshift_invoice"
script_path_rs = "D:\\oracle\\daily_invoice_count_check\\scripts_files_redshift"
daily_files = "D:\\oracle\\daily_invoice_count_check\\daily_files"
trigger_dir = "D:\\oracle\\daily_invoice_count_check\\trigger_files"
trigger_s3_counts="s3://desototech/DWH_team/sid/Daily_invoice_trigger/invoice_count_trigger/"
trigger_s3_cbi="s3://desototech/Daily_invoice_trigger/cbi_procedures_trigger/"
log_file_email = f"{log_dir}\\email_notication_{run_date}.log"

start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

vprd_fivetran_sync_runtime = "0015"

#sys.stdout = open(f"{log_dir}\\daily_invoice_count_extract_{run_date}.log" ,'a')

print("start time : ", start_time)
print("run date : ", run_date)  
print("prev date : ", prev_day)  

listOfGlobals = globals()
listOfGlobals['start_time'] = start_time
listOfGlobals['trigger_dir'] = trigger_dir
listOfGlobals['trigger_s3_counts'] = trigger_s3_counts
listOfGlobals['main_path'] = main_path
listOfGlobals['log_dir'] = log_dir
listOfGlobals['run_date'] = run_date
listOfGlobals['daily_files'] = daily_files
listOfGlobals['trigger_s3_cbi'] = trigger_s3_cbi
listOfGlobals['vprd_fivetran_sync_runtime'] = vprd_fivetran_sync_runtime
listOfGlobals['log_file_email'] = log_file_email

def vprd_fivetran_sync_steps():
    print("************************************************************") 
    print("function  called - vprd_fivetran_sync_steps") 
	
    run_dt = datetime.now().strftime('%Y%m%d')
    prev_day = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
	
    print("run date : ", run_dt)
    print("prev date : ", prev_day) 
	
    os.chdir(main_path)
    
    # Define the file path for logging
    log_file_vprd = f"{log_dir}\\log_vprd_{run_dt}.log"
 
    with open(log_file_vprd, "w") as f:
       subprocess.run(["python",f"vprd_generic_ext.py", "-d", run_dt, "-c" , "extorclist.config", "-f", f"D:\\oracle\\daily_invoice_count_check\\scripts_files_vprd\\sql_manual_extract_vprd.lst"], stdout=f) 
    
    time.sleep(5)
    
    # Define the file path for logging
    log_file_redshift = f"{log_dir}\\log_redshift_{run_dt}.log"
    #
    #
    with open(log_file_redshift, "w") as f:
       subprocess.run(["python",f"redshift_generic_ext.py", "-d", run_dt, "-c", "redshift_conn.config", "-f", f"D:\\oracle\\daily_invoice_count_check\\scripts_files_redshift\\sql_manual_extract_redshift.lst"], stdout=f) 
    
    time.sleep(5)
	
    
    while True:
    # Open the file and read its content
        with open(log_file_vprd, 'r') as file:
            log_file_vprd_content = file.read()
        
        with open(log_file_redshift, 'r') as file:
            log_file_redshift_content = file.read()

    # Use regular expressions to find the word 'complete'
        match1 = re.search(r'complete', log_file_vprd_content)
        match2 = re.search(r'complete', log_file_redshift_content)

        if match1 and match2:
            print("extaction completed Successfully")
            print("completion time : ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            break
        time.sleep(1)  # Adjust as needed
    
    #Checking difference between VPRD and Redshift,store it to file
    difference_file_path = f"{daily_files}\\{run_dt}\\"
    os.chdir(difference_file_path)
    
    result = subprocess.run("fc /b max_invoice_number_and_date_vprd.csv max_invoice_number_and_date_redshift.csv", shell=True, capture_output=True, text=True)
    # Check if the command was successful
    if result.returncode != 0:
        # There is a difference, print the output to the file
        with open("diff_invoice_" + run_dt + ".txt", "w") as f:
            f.write(result.stdout)
    elif "no differences encountered" in result.stdout.lower():
    # No difference, create an empty file
        open("diff_invoice_" + run_dt + ".txt", "w").close()
    else:
    # There may be some other output, print it to the file
        with open("diff_invoice_" + run_dt + ".txt", "w") as f:
            f.write(result.stdout)
    
    #subprocess.run("fc /b max_invoice_number_and_date_vprd.csv max_invoice_number_and_date_redshift.csv > diff_invoice_"+ run_dt + ".txt", shell=True)
    print("extract total size of the file and store into variable") 
	#file_size_diff_file=$(ls -l diff_invoice_${run_dt}.txt | awk '{print $5}')
	
    file_name = f"diff_invoice_{run_dt}.txt"
    file_size = os.path.getsize(file_name)
    print("File size:", file_size)
    
    if file_size != 0:
        mismatch_statement = "count mismatched"
        log_file = f"{log_dir}\\wrapper_{run_dt}.log"
        with open(log_file,'w') as file:
            file.write(mismatch_statement) 
        
        diff_invoice = "diff_invoice_"+ run_dt + ".txt"    
        with open(diff_invoice, 'r') as file:
            diff_invoice_content = file.read()
        
        print(diff_invoice_content) 
        
        
        with open("max_invoice_number_and_date_vprd.csv", 'r') as file:
            Source_Counts = file.read()
            
        with open("max_invoice_number_and_date_redshift.csv", 'r') as file:
            Destination_Counts = file.read()
			
        email_start_subject = f"Alert! Daily Invoice mismatch found on {run_dt}"
        print(email_start_subject)
        email_start_body = f"""
There is mismatch between vprd and Redshift invoice count.

Header : max_invoice_num|max(invo_created_date)|max(invo_mod_date)

VPRD  : {Source_Counts}
Redshift : {Destination_Counts}


Start time : {start_time}
Completion time : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        """
        #print(email_start_body)
        config_file = f"{main_path}\\email_config.config"

        with open(log_file_email, "a") as f:
            subprocess.run(["python",f"{main_path}\\email_notification.py", "--s",email_start_subject ,"--b",email_start_body, "--c",config_file], stdout=f)
        Redshift_Counts=''    
    else:
        match_statement = "count matched"
        log_file = f"{log_dir}\\wrapper_{run_dt}.log"
        with open(log_file,'w') as file:
            file.write(match_statement) 
                
        with open(f"{log_dir}\\wrapper_{run_dt}.log", 'r') as file:
                match_statement_log = file.read()
                
        match = re.search(r'count matched', match_statement_log)
        
        if match:
            print("Richter and Redshift invoice count matched")
            os.chdir(f"D:\\oracle\\daily_invoice_count_check")
            
            log_file_vprd_generic = f"{log_dir}/vprd_count_{run_dt}.log"
    
    
            with open(log_file_vprd_generic, "w") as f:
                subprocess.run(["python",f"vprd_generic_ext.py", "-d", run_dt, "-c" , "extorclist.config", "-f", f"D:\\oracle\\daily_invoice_count_check\\scripts_files_vprd\\sql_vprd_invoice_count.lst"], stdout=f)             
            time.sleep(10)
            
            # Define the file path for logging
            log_file_redshift_generic = f"{log_dir}/redshift_count_{run_dt}.log"
        
        
            with open(log_file_redshift_generic, "w") as f:
                subprocess.run(["python",f"redshift_generic_ext.py", "-d", run_dt, "-c", "redshift_conn.config", "-f", f"D:\\oracle\\daily_invoice_count_check\\scripts_files_redshift\\sql_redshift_invoice_count.lst"], stdout=f) 
        
            time.sleep(10)
            
            os.chdir(difference_file_path)
            with open(f"vprd_invoice_count.csv", 'r') as file:
                vprd_Counts = file.read()
            
            with open(f"redshift_count.csv", 'r') as file:
                Redshift_Counts = file.read()
            
            with open(f"max_invoice_number_and_date_vprd.csv", 'r') as file:
                Source_Counts = file.read()
            
            with open(f"max_invoice_number_and_date_redshift.csv", 'r') as file:
                Destination_Counts = file.read()
            
            print("vprd_count: ", vprd_Counts)
            print("Redshift_count: ", Redshift_Counts)
            print("Source_Counts: ", Source_Counts)
            print("Destination_Counts: ", Destination_Counts)
            
            email_start_subject = f"Daily Invoice count matched on  {run_dt}"
            #print(email_start_subject)
            email_start_body = f"""
Today's max invoice count and time have been matched for both source and destination.
Header : max_invoice_num|max(invo_created_date)|max(invo_mod_date)

VPRD  : {Source_Counts}
Redshift : {Destination_Counts}

Please find below total number of invoices to be processed today:

VPRD_invoice_count : {vprd_Counts}
Redshift_invoice_count : {Redshift_Counts}

Start time : {start_time}
Completion time : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        """
        #print(email_start_body)
            config_file = f"{main_path}//email_config.config"
    
            with open(log_file_email, "a") as f:
                subprocess.run(["python",f"{main_path}\\email_notification.py", "--s",email_start_subject ,"--b",email_start_body, "--c",config_file], stdout=f)
	
	
    #calling checking_count_matched
    checking_count_matched(Redshift_Counts)
        
def checking_count_matched(Redshift_Counts = ''):
	
    print("function  called - checking_count_matched")
	
    run_dt = datetime.now().strftime('%Y%m%d')
    invoice_counts=Redshift_Counts
    
    log_file = f"{log_dir}\\wrapper_{run_dt}.log"
	
    with open(log_file, 'r') as file:
           match_statement = file.read()
           
    match = re.search(r'count matched', match_statement)
	
    if match:
        print("There is no mismatch found between VPRD and Redshift invoice counts")
        
        with open(f"{trigger_dir}\\invoice_counts_matched_trigger_{run_dt}.txt", 'a'):
            os.utime(f"{trigger_dir}\\invoice_counts_matched_trigger_{run_dt}.txt", None)
            
        subprocess.run(["aws", "s3", "cp", trigger_dir +"\\invoice_counts_matched_trigger_"+run_dt+".txt", trigger_s3_counts],  stderr=subprocess.STDOUT, shell=True)
        #subprocess.run(["aws", "s3", "cp", trigger_dir +"/cbi_procedures_trigger_"+run_date+".txt", trigger_s3_cbi], stderr=subprocess.STDOUT, shell=True)
        print("invoice count matched trigger file is copied successfully on S3")
        
        #sending message on teams
        teams_message(invoice_counts)
        return
        
    else:
		#calling wait_for_count_matched function for 5 min sleep
        print("waiting for count matched")
        wait_for_count_matched()
        
        
def teams_message(invoice_counts):
    print("************************************************************")
    print("function  called - teams_message")
    print("************************************************************")
    
    message_1="Invoice count matched between Source(VPRD) and Fivetran."
    message_2=f"Total " + str(invoice_counts) +" invoices today for ETL delta loads"
    
    #echo $1 $2 $3 
    print(message_2)
    
    payload1 = {"text": message_1}
    payload2 = {"text": message_2}
    
    
    # Convert payload to JSON string
    payload_str1 = json.dumps(payload1)
    payload_str2 = json.dumps(payload2)
    # Construct the curl command without the JSON payload
    # curl_command1 = ["curl", "-H", "Content-Type: application/json", "-d", payload_str1, "https://o365spi.webhook.office.com/webhookb2/769fb003-054d-4161-8e90-54b0e5f48576@bdeeee28-22ab-472f-8510-87812e5557e1/IncomingWebhook/f1418fc8d5ed4b6695279ad3c2bf5095/329a3b08-1316-452c-a496-6ab47e68127d"]
    # curl_command2 = ["curl", "-H", "Content-Type: application/json", "-d", payload_str2, "https://o365spi.webhook.office.com/webhookb2/769fb003-054d-4161-8e90-54b0e5f48576@bdeeee28-22ab-472f-8510-87812e5557e1/IncomingWebhook/f1418fc8d5ed4b6695279ad3c2bf5095/329a3b08-1316-452c-a496-6ab47e68127d"]
    
    curl_command1 = ["curl", "-H", "Content-Type: application/json", "-d", payload_str1, "https://o365spi.webhook.office.com/webhookb2/769fb003-054d-4161-8e90-54b0e5f48576@bdeeee28-22ab-472f-8510-87812e5557e1/IncomingWebhook/f1418fc8d5ed4b6695279ad3c2bf5095/329a3b08-1316-452c-a496-6ab47e68127d/V2zv9UjiDCrHp0NkZoRwHN0CT_RyC8NMEbQesPK8yfTlI1"]
    curl_command2 = ["curl", "-H", "Content-Type: application/json", "-d", payload_str2, "https://o365spi.webhook.office.com/webhookb2/769fb003-054d-4161-8e90-54b0e5f48576@bdeeee28-22ab-472f-8510-87812e5557e1/IncomingWebhook/f1418fc8d5ed4b6695279ad3c2bf5095/329a3b08-1316-452c-a496-6ab47e68127d/V2zv9UjiDCrHp0NkZoRwHN0CT_RyC8NMEbQesPK8yfTlI1"]
    # Run the curl command
    subprocess.run(curl_command1)
    time.sleep(5)
    subprocess.run(curl_command2)
    time.sleep(5)
	
	#calling checking count function to see counts are matched or not, if it's matched then create trigger file on S3
    print("#####################################################################################################################")
    
    
def wait_for_count_matched():
    print("************************************************************")
    print("going to sleep for 5 minutes")
    print("************************************************************")
    time.sleep(300)
	
	#calling vprd_fivetran_sync_steps to verify invoice count again between VPRD and Redshift
    print("#####################################################################################################################")
    vprd_fivetran_sync_steps()


def cbi_procedures():
    print("************************************************************")
	
    os.chdir(main_path)
	
    
    log_file_vprd_generic = f"{log_dir}\\cbi_procedures_{run_date}.log"
 
 
    with open(log_file_vprd_generic, "w") as f:
        subprocess.run(["python",f"vprd_generic_ext.py", "-d", run_date, "-c" , "extorclist.config", "-f", f"D:\\oracle\\daily_invoice_count_check\\scripts_files_vprd\\sql_cbi_procedures_message.lst"], stdout=f) 
    
    while True:
        with open(log_file_vprd_generic, 'r') as file:
            match_statement = file.read()
        
        match = re.search(r'complete', match_statement)
        
        if match:
            print("python for cbi_procedures completed Successfully")
            print(f"Completion time : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            break
        else:
            time.sleep(10)
	
    with open(f"{daily_files}\\{run_date}\\cbi_procedures_message.csv", 'r') as file:
            match_statement = file.read()
    
    match = re.search(r'ok', match_statement,re.IGNORECASE)
        
    if match:
        print("CBI PROCEDURES is completed")
        with open(f"{trigger_dir}\\cbi_procedures_trigger_{run_date}.txt", 'a'):
            os.utime(f"{trigger_dir}\\cbi_procedures_trigger_{run_date}.txt", None)
            
        time.sleep(10)
        #subprocess.run(["aws", "s3", "cp", trigger_dir +"/cbi_procedures_trigger_{run_date}.txt" trigger_s3_cbi], stderr=subprocess.STDOUT, shell=True)
        subprocess.run(["aws", "s3", "cp", trigger_dir +"\\cbi_procedures_trigger_"+run_date+".txt", trigger_s3_cbi], stderr=subprocess.STDOUT, shell=True)
        print("cbi_procedures trigger file is copied successfully on S3")
        print("************************************************************")
        print("calling credit_memo_and_invoices_count function to check Today's Creadit Memo and Invoice counts")
        credit_memo_and_invoices_count()
        return
    else:
        print("CBI PROCEDURE is not completed")
        email_start_subject = f"Alert! CBI PROCEDURE is not completed on {run_date}"
        #print(email_start_subject)
        email_start_body = f"""
CBI PROCEDURE is not completed yet for SalesJournal.

Completion time : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        """
        #print(email_start_body)
        config_file = f"{main_path}\\email_config.config"
    
        with open(log_file_email, "a") as f:
            subprocess.run(["python",f"{main_path}\\email_notification.py", "--s",email_start_subject ,"--b",email_start_body, "--c",config_file], stdout=f)
            
        time.sleep(10)
        error_check()
            
#Checking Error for CBI_procedures 
def error_check():
    print("************************************************************")
    print("function  called - error_check")
    print("************************************************************")
    
    os.chdir(main_path)
    
    log_file_vprd_generic = f"{log_dir}\\error_check_{run_date}.log"
 
 
    with open(log_file_vprd_generic, "w") as f:
        subprocess.run(["python",f"vprd_generic_ext.py", "-d", run_date, "-c" , "extorclist.config", "-f", f"D:\\oracle\\daily_invoice_count_check\\scripts_files_vprd\\sql_error_check.lst"], stdout=f) 
        time.sleep(10)
    
    while True:
        with open(log_file_vprd_generic, 'r') as file:
            match_statement = file.read()
        match = re.search(r'complete', match_statement)
        
        if match:
            print("Python for Error check completed Successfully")
            print(f"Completion time : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            break
        else:
            time.sleep(10)
	
    os.chdir(f"{daily_files}\\{run_date}\\")
    
    
    file_name = f"error_check.csv"
    file_size = os.path.getsize(file_name)
    print("File size:", file_size)
    
    if file_size == 0:
        print("There is no any error found")
    else:
        print("Hey.!!! Error found !!")
        print("Sending error notification on teams")
        
        error_message="Alert! Error found in CBI_procedures for invoice generation process. Kindly check your email for the same"
        payload1 = {"text": error_message}
        payload_str1 = json.dumps(payload1)
        # curl_command1 = ["curl", "-H", "Content-Type: application/json", "-d", payload_str1, "https://o365spi.webhook.office.com/webhookb2/769fb003-054d-4161-8e90-54b0e5f48576@bdeeee28-22ab-472f-8510-87812e5557e1/IncomingWebhook/f1418fc8d5ed4b6695279ad3c2bf5095/329a3b08-1316-452c-a496-6ab47e68127d"]
        
        curl_command1 = ["curl", "-H", "Content-Type: application/json", "-d", payload_str1, "https://o365spi.webhook.office.com/webhookb2/769fb003-054d-4161-8e90-54b0e5f48576@bdeeee28-22ab-472f-8510-87812e5557e1/IncomingWebhook/f1418fc8d5ed4b6695279ad3c2bf5095/329a3b08-1316-452c-a496-6ab47e68127d/V2zv9UjiDCrHp0NkZoRwHN0CT_RyC8NMEbQesPK8yfTlI1"]
        subprocess.run(curl_command1)
        time.sleep(5)
        
        print("Sending mail notification for error")
        msg = "There is an error found while processing CBI_procedure for SalesJournal:"
        print(msg)
        email_start_subject = f"Alert! Error found for CBI_procedure SalesJournal on {run_date}"
        #print(email_start_subject)
        email_start_body = f"""
There is an error found while processing CBI_procedure for SalesJournal:

Completion time : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        """
        #print(email_start_body)
        config_file = f"{main_path}\\email_config_with_tim.config"
        attachmnet_path = f"{daily_files}\\{run_date}\\error_check.csv"
        with open(log_file_email, "a") as f:
            subprocess.run(["python",f"{main_path}\\email_notification_with_attachment.py", "-s",email_start_subject ,"-b",email_start_body, "-c", config_file, "-u", attachmnet_path], stdout=f)
        
	
	#calling cbi_sleep to wait till CBI_procedure is completed
    print("#####################################################################################################################")
    cbi_sleep()
        
	
def credit_memo_and_invoices_count():
    print("************************************************************")
    
    os.chdir(main_path)
    
    log_file_vprd_generic = f"{log_dir}\\CM_and_IN_invoice_counts_{run_date}.log"
    with open(log_file_vprd_generic, "w") as f:
        subprocess.run(["python",f"vprd_generic_ext.py", "-d", run_date, "-c" , "extorclist.config", "-f", f"D:\\oracle\\daily_invoice_count_check\\scripts_files_vprd\\sql_CM_and_IN_invoice_counts.lst"], stdout=f) 
        time.sleep(10)
    
    while True:
        with open(log_file_vprd_generic, 'r') as file:
            match_statement = file.read()
        match = re.search(r'complete', match_statement)
        
        if match:
            print("python for CM_and_IN_invoice_counts completed Successfully")
            print(f"Completion time : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            break
        else:
            time.sleep(10)
	
	
	
    os.chdir(f"{daily_files}\\{run_date}\\")
	
    print("Checking for Credit Memo and Invoice count")
    #changing '|' to '-' using sed command
    filename = 'CM_and_IN_invoice_counts.csv'
    with fileinput.FileInput(filename, inplace=True) as file:
        for line in file:
            print(line.replace('|', '-'), end='')
            
    time.sleep(10)
    
    #Storing invoice and Credit Memo counts in variable 
    invoice_counts = []
    CM_counts = []
	
    with open(filename, 'r') as file:
        reader1 = csv.reader(file)
    
        for i in reader1:
            value = i[0]
            if 'invoice' in value.lower():
                invoice_counts.append(value)
			
    with open(filename, 'r') as file:
        reader2 = csv.reader(file)
        
        for i in reader2:
            value = i[0]
            if 'credit_memo' in value.lower():
                CM_counts.append(value)
	
	
    final_message=f"{invoice_counts[0]} and {CM_counts[0]} in VPRD for {run_date}"
    print(final_message)
	
    print("Sending message in Teams for CM and IN counts")
    
    payload1 = {"text": final_message}
    payload_str1 = json.dumps(payload1)
    # curl_command1 = ["curl", "-H", "Content-Type: application/json", "-d", payload_str1, "https://o365spi.webhook.office.com/webhookb2/769fb003-054d-4161-8e90-54b0e5f48576@bdeeee28-22ab-472f-8510-87812e5557e1/IncomingWebhook/f1418fc8d5ed4b6695279ad3c2bf5095/329a3b08-1316-452c-a496-6ab47e68127d"]
    curl_command1 = ["curl", "-H", "Content-Type: application/json", "-d", payload_str1, "https://o365spi.webhook.office.com/webhookb2/769fb003-054d-4161-8e90-54b0e5f48576@bdeeee28-22ab-472f-8510-87812e5557e1/IncomingWebhook/f1418fc8d5ed4b6695279ad3c2bf5095/329a3b08-1316-452c-a496-6ab47e68127d/V2zv9UjiDCrHp0NkZoRwHN0CT_RyC8NMEbQesPK8yfTlI1"]
    subprocess.run(curl_command1)
    time.sleep(5)

	#calling check_run_date function
    print("calling check_run_date function to check whether run_date is changed or not")
    check_run_date()
    
    
def check_run_date():
    print("************************************************************")
    print("function  called - check_run_date")
    today = datetime.now().strftime('%Y%m%d')
    #today='20240515'
    print("run_date is : ", run_date)
    print("current_date is : ", today)
	
    if run_date != today:
        print("************************************************************")
        print("Date is changed - calling check_time function to verfiy vprd_fivetran_sync_steps run time is excceded 0010 or not")
        check_time()
    else:
        #calling download_log_s3 to download parent_log file again
        print("************************************************************")
        print("calling function - run_date_check_sleep")
        run_date_check_sleep()
    
    
def run_date_check_sleep():
    print("************************************************************")
    print("going to sleep for 5 minutes")
    print("************************************************************")
    time.sleep(300)
    
    #calling check_run_date function to check current date
    print("#####################################################################################################################")
    check_run_date()
    
    
def check_time():
    print("************************************************************")
    print("function  called - check_time")
    
    current_time = datetime.now().strftime('%H%M')
    print("current time is :" ,current_time)
    print("vprd_fivetran_sync_runtime is :", vprd_fivetran_sync_runtime)
    
    if vprd_fivetran_sync_runtime >= current_time :
        print("************************************************************")
        print("vprd_fivetran_sync_runtime is not matched yet.Hence,going to sleep")
        check_time_sleep()
    else:
        #calling download_log_s3 to download parent_log file again
        print("The current_time has exceeded the vprd_fivetran_sync_runtime")
        print("************************************************************")
        print("calling function - vprd_fivetran_sync_steps")
        vprd_fivetran_sync_steps()
        
        
#cbi_proceducer sleep for 10 min
def cbi_sleep():
	print("************************************************************")
	print("going to sleep for 10 minutes")
	print("************************************************************")
	time.sleep(600)
	
	#calling cbi_procedures to verify "OK" message
	print("#####################################################################################################################")
	cbi_procedures()
    
def check_time_sleep():
    print("************************************************************")
    print("going to sleep for 5 minutes")
    print("************************************************************")
    time.sleep(300)
	
	#calling check_time function
    print("#####################################################################################################################")
    check_time()
    
#Invoke your function
print("function  called - cbi_procedures")
cbi_procedures()