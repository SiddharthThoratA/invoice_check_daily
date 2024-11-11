import sys, getopt
import datetime
import csv
import gzip
import psycopg2
import configparser
import smtplib
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders

"""
NAME: email_notification.py
CREATED DATE: 05-Apr-2024
Created By : Dixit Goswami
DESCRIPTION: Emai script for sending start and end mail
================================================================================================================
"""

#run_date = datetime.datetime.now().strftime("%Y%m%d")
#run_date = ''

"""
how to call this script :
python3 email_notification.py -s <subject> -b <body> -c <config>
example :
python3 email_notification.py -s <subject> -b <body> -c <config>
"""

def main(argv):

    #run_dt = 'date +%Y%m%d%H%M'
    try:
        opts, args = getopt.getopt(argv, "hs:b:c:u:", ["subject=", "body=", "config=", "attached="])
    except getopt.GetoptError:
        print('email_notification.py -s <subject> -b <body> -c <config> -u <attached>' )
    for opt, arg in opts:
        if opt == '-h':
            print('email_notification.py -s <subject> -b <body> -c <config> -u <attached>')
            sys.exit(2)
        elif opt in ('-s','--subject'):
            subject = arg
            #print('subject here :',subject)
        elif opt in ('-b', '--body'):
            body = arg
            #print('body here :',body)
            if not body:
                print('Missing body  --> syntax is : email_notification.py -s <subject> -b <body> -c <config> -u <attached>')
                sys.exit()
        elif opt in ('-c', '--config'):
            config_file_email = arg
            if not config_file_email:
                print('Missing config file name --> syntax is : email_notification.py -s <subject> -b <body> -c <config> -u <attached>')
                sys.exit()
        elif opt in ('-u', '--attached'):
            attachment_path = arg
            if not attachment_path:
                print('Missing config file name --> syntax is : email_notification.py -s <subject> -b <body> -c <config> -u <attached>')
                sys.exit()

    print('email subject is :', subject)
    print('email body is :', body)
    print('email attachment pathis :', attachment_path)

    listOfGlobals = globals()
    #listOfGlobals['run_date'] = run_dt
    listOfGlobals['email_subject'] = subject
    listOfGlobals['email_body'] = body
    listOfGlobals['email_attachment'] = attachment_path
    

    email_config = read_email_config(config_file_email)
    #print(email_config)
    
    send_email(email_config, email_subject, email_body, attachment_path)

def read_email_config(config_file):
    config = configparser.ConfigParser()
    config.read(config_file)
    
    email_config = {
        "sender_email": config.get("Email", "sender_email"),
        "receiver_email": config.get("Email", "receiver_email"),
        "smtp_server": config.get("Email", "smtp_server"),
        "smtp_port": config.getint("Email", "smtp_port"),
        "smtp_username": config.get("Email", "smtp_username"),
        "smtp_password": config.get("Email", "smtp_password")
    }
    
    return email_config

def send_email(email_config, email_subject, email_body, attachment_path):
#def send_email(email_config):
    sender_email = email_config["sender_email"]
    receiver_email = email_config["receiver_email"].split(',')
    smtp_server = email_config["smtp_server"]
    smtp_port = email_config["smtp_port"]
    smtp_username = email_config["smtp_username"]
    smtp_password = email_config["smtp_password"]
    
    email_body = MIMEText(email_body, "plain")
    
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = ",".join(receiver_email)
    message["Subject"] = email_subject
    message.attach(email_body)
    
    with open(attachment_path, "rb") as attachment:
        part = MIMEBase("application", "octet-stream")
        part.set_payload(attachment.read())
        encoders.encode_base64(part)
        file_name = os.path.basename(attachment_path)
        part.add_header("Content-Disposition",f"attachment; filename= {file_name}",
        )
        message.attach(part)
    
    try:
        smtp_server = smtplib.SMTP(smtp_server, smtp_port)
        smtp_server.starttls()
        smtp_server.login(smtp_username, smtp_password)
        smtp_server.sendmail(sender_email, receiver_email, message.as_string())
        smtp_server.quit()
        print("Email sent successfully!")
    except Exception as e:
        print("Failed to send email:", e)

if __name__ == "__main__":
    main(sys.argv[1:])
