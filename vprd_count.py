import sys, getopt
import datetime
import cx_Oracle
import csv
import os
import gzip
import shutil
#from datetime import datetime
from datetime import datetime,timedelta

"""
how to call this script :
python3 orc_ext_conf_list.py -c <config_file> -f <sql_file_list>
example :
python3 orc_ext_conf_list.py -c extorclist.config -f /home/kartik/python_prog/list2.lst
"""

def main(argv):

    # now = datetime.datetime.now()
    # print('current time :', datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    try:
        opts, args = getopt.getopt(argv, "hd:f:c:", ["date=","config=", "file="])
    except getopt.GetoptError:
        print('orc_ext_conf_list.py -c <config_file> -f <sql_list_file>')
    for opt, arg in opts:
        if opt == '-h':
            print('orc_ext_conf_list.py -d <rundate> -c <config_file> -f <sql_list_file>')
            sys.exit(2)
        elif opt in ('-d','date='):
            run_dt = arg
        elif opt in ('-c', '--config'):
            config_file = arg
            if not config_file:
                print('Missing config file name --> syntax is : orc_ext_conf_list.py -c <config_file> -f <sql_list_file>')
                sys.exit()
        elif opt in ('-f', '--file'):
            list_file = arg
            if not list_file:
                print('Missing config file name --> syntax is : orc_ext_conf_list.py -c <config_file> -f <sql_list_file>')
                sys.exit()

    print('config file is :', config_file)
    print('sql list file is :', list_file)
    print('run date is :', run_dt)

    curr_date = datetime.strptime(run_dt, '%Y%m%d').date()
    curr_date = str(curr_date)
    print('curr_date is ',curr_date)

    currentTimeDate = datetime.now() - timedelta(days=1)
    prev_dt = currentTimeDate.strftime('%Y-%m-%d')
    print('prev_dt is: ',prev_dt)


    listOfGlobals = globals()
    listOfGlobals['run_date'] = run_dt

    listOfGlobals['prev_dt'] = prev_dt

    p_dt = str(prev_dt)
    print('p_dt is : ',p_dt)

    listOfGlobals['curr_date'] = curr_date

    #ex_sql(sql, ofile)
    #print("Done!")
    read_config(config_file, list_file)

def read_config(config_file, list_file):
    conf_file = config_file
    fileobj = open(conf_file)
    params = {}
    for line in fileobj:
        line = line.strip()
        if not line.startswith('#'):
            conf_value = line.split('=')
            if len(conf_value) == 2:
                params[conf_value[0].strip()] = conf_value[1].strip()
    fileobj.close()

    params.update(SQL_LIST_FILE = list_file)
    #print(params)

    ex_sql_file(params)

    print('state : complete')


#def ex_sql(sql, ofile):
def ex_sql_file(params):
    #print('inside ex_sql_file()')

    if params['USER'] and params['PWD'] and params['ORA_HOST'] and params['SID'] and params['ORA_PORT']:
        DSN = params['USER'] + '/' + params['PWD'] +'@' + params['ORA_HOST'] + ':' + params['ORA_PORT'] + '/' + params['SID']
    #print(DSN)

    if params['OUT_FILE_LOC']:
        ofilepath = params['OUT_FILE_LOC'].strip()
        ofilepath = ofilepath.replace('RUN_DATE',run_date)
        print(ofilepath)

        try:
            os.mkdir(ofilepath)
        except OSError:
            print ("Creation of the directory %s failed" % ofilepath)
        else:
            print ("Successfully created the directory %s " % ofilepath)
    else:
        print('Missing output location path --- exiting')
        sys.exit()

    if params['SQL_LIST_FILE']:
        sql_list_file = params['SQL_LIST_FILE'].strip()
    else:
        print('There is not sql file to process --- exiting')
        sys.exit()


    sql_list_read = open(sql_list_file)
    for line in sql_list_read:
        sql_file = line.strip()
        print('sql file is : ', sql_file)
        ofile = ofilepath + '/' + str(sql_file).split('.')[0].split('/')[-1] + '.csv'
        print('outputfile :',ofile)
        f = open(sql_file, "r")
        sql = f.read().split(";")[0]
        print('sql is: ',sql)
       
        char_to_replace = {'Prev_dt': prev_dt,'Run_dt': curr_date}

        for key, value in char_to_replace.items():
    # Replace key character with value character in string
            sql = sql.replace(key, value)
        #sql = sql.replace('Prev_dt',prev_dt)
        #sql = sql.replace(P_dt,p_dt)
            print('sql is: ',sql)
        f.close()
        if not sql:
            print('Missing sql file/s ... exiting')
            sys.exit()

        #print('Executing for :', sql_file)
       # print('start time :', datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        ex_sql(sql, ofile, DSN)
       # print('completion time :', datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

        #print('start zip...')
        #ozipfile = str(ofile) + '.gz'
        #with open(ofile, 'rb') as f_in:
            #with gzip.open(ozipfile, 'wb') as f_out:
                #shutil.copyfileobj(f_in, f_out)
        #print('output gzip file :', ozipfile)
        #print('zip completion time :', datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        print('------------------------------------------------------------------------------------')

def ex_sql(sql, ofile, DSN):

    #creating oracle connection
    conn = cx_Oracle.connect(DSN)
    cursor = conn.cursor()

    csv_file = open(ofile, "w")
    writer = csv.writer(csv_file, delimiter='|', lineterminator="\n", quoting=csv.QUOTE_NONE, escapechar='\\')
    #writer = csv.writer(csv_file)

    cursor.execute(sql)
    for row in cursor:
        # print(row)
        writer.writerow(row)

    cursor.close()
    conn.commit()

    #print("Checking connection version")
    #print(conn.version)

    conn.close()
    csv_file.close()



if __name__ == "__main__":
    main(sys.argv[1:])

