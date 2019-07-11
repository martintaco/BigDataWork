import boto3, json, pprint, requests, textwrap, time, logging, requests
from datetime import datetime, timedelta
import os
import sys
import pyodbc
import pandas as pd
import pexpect
import botocore

start_time = datetime.now()
print("Inicio : ", start_time.isoformat())

#host_emr = 'http://ec2-10-12-4-203.compute-1.amazonaws.com:8998'
#host_emr = 'http://ec2-10-12-6-191.compute-1.amazonaws.com:8998'
host_emr = 'http://ec2-10-12-4-177.compute-1.amazonaws.com:8998'

def track_statement_progress(host, id):
    statement_status = ''
    final_statement_status = ''
    message=""
    # Poll the status of the submitted scala code
    while (statement_status != 'success' and statement_status != 'dead'):
    # If a statement takes longer than a few milliseconds to execute, Livy returns early and provides a statement URL that can be polled until it is complete:
            statement_url = host + '/batches/' + str(id)
            statement_response = requests.get(statement_url, headers={'Content-Type': 'application/json'})
            statement_status = statement_response.json()['state']
            logging.info('Statement status: ' + statement_status)

            #logging the logs
            lines = requests.get(statement_url + '/log', headers={'Content-Type': 'application/json'}).json()['log']
            for line in lines:
                    logging.info(line)
            time.sleep(10)
    if statement_status == 'success':
            #curl -X DELETE localhost:8998/batches/53
            requests.delete(statement_url, headers={'Content-Type': 'application/json'})
            final_statement_status = 'success'
            message = "Livy execution success"
    if statement_status == 'dead':
            requests.delete(statement_url, headers={'Content-Type': 'application/json'})
            final_statement_status = 'dead'
            #logging.info('Statement exception: ' + lines.json()['log'])
            #for trace in statement_response.json()['output']['traceback']:
            #        logging.info(trace)
            #raise ValueError('Final Statement Status: ' + final_statement_status)
            message = "Livy execution is dead"
    logging.info('Final Statement Status: ' + final_statement_status)
    return message

## Crear Variable ##
Command1 = sys.argv[1]
Command2 = sys.argv[2]
Command3 = sys.argv[3]

#lanza apache livy to emr cluster
host = host_emr
data = {"className": "pe.com.belcorp.runMDM",
"args": ["--env", Command1, "--date", Command2, "--source",Command3],
"file": "s3://belc-bigdata-functional-dlk-qas/analitico/jars/MDMinformation-assembly-0.1.jar"}
headers = {'Content-Type': 'application/json'}
r = requests.post(host + '/batches', data=json.dumps(data), headers=headers)
id = r.json()['id']
msg = track_statement_progress(host, id)

#spark-submit --num-executors 4 --executor-cores 4 --executor-memory 5g --class pe.com.belcorp.tactica.main.IntakeDaily /home/hadoop/tactica/target/scala-2.11/tactica-assembly-0.1.jar --url "jdbc:redshift://belc-bigdata-qas.c8evymt0o1da.us-east-1.redshift.amazonaws.com:5439/analitico?user=lgrados&password=Belcorp2018" --schema bigdatadatalake_functional_qas --tempS3Dir "s3n://belc-bigdata-functional-dlk-qas/analitico/Redshift" --country "PE" --campania "201810" --location "s3://belc-bigdata-functional-dlk-qas/analitico/GlueDatabase/functional"
end_time = datetime.now()
print("Fin: ", end_time.isoformat())
