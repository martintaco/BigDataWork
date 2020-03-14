import boto3, json, pprint, requests, textwrap, time, logging, requests
from datetime import datetime, timedelta
import os
import sys
import pyodbc 
import pandas as pd
import pexpect
import botocore


host_emr = 'http://ec2-10-12-6-191.compute-1.amazonaws.com:8998'

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
            #requests.delete(statement_url, headers={'Content-Type': 'application/json'})
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

conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=DATABASE=BD_ANALITICO;UID=;PWD=')

cursor = conn.cursor()

cursor.execute("select codpais, aniocampanaexpo, iteracion=max(iteracion), idprocess=max(idprocess) from dbo.ARP_Configuracion_Ejecucion_Campaign where estado = 3 and parquet is null group by aniocampanaexpo, codpais")

print("1 query")

# In[5]:


df =[]

names = [ x[0] for x in cursor.description]
rows = cursor.fetchone()
if rows:
    df.append([rows[0], rows[1], rows[2], rows[3]])
    
df_output_ofertas = pd.DataFrame(df, columns=names)    


# In[6]:


df_2 =[]
try:
    for index, row_meta in df_output_ofertas.iterrows():
        codpais=row_meta[0]
        aniocampana=row_meta[1]
        iteracion = row_meta[2]
        idprocess = row_meta[3]
                
        params = (codpais, aniocampana)
        
        print(codpais, aniocampana)
        
        #Genera csv output campaign en ruta compartida pents140\bigdata\recommender
        cursor.execute("{CALL pCopyARPOutputToCSV (?,?)}", params)
        time.sleep(2100)
        print("csv created")
        
        
        #upload csv output campaign desde ec2 hacia S3
        os.system('aws s3 cp /home/bigdatateam/ftpshare/Recommender/arp_output.csv s3://belc-bigdata-apps-shared-qas/recommender/output/tmp_arp_output_campaign/') 
        
        #lanza apache livy to emr cluster
        host = host_emr
        data = {"className": "pe.com.belcorp.recomender.main.generateParquet",
        "args": ["--iteration", iteracion, "--country", codpais, "--campania", aniocampana, "--path-s3", "s3://belc-bigdata-apps-shared-qas/recommender/output/", "--idprocess", idprocess ],
        "file": "s3://belc-bigdata-functional-dlk-qas/analitico/jars/recommender-assembly-0.1.jar"}
        headers = {'Content-Type': 'application/json'}
        r = requests.post(host + '/batches', data=json.dumps(data), headers=headers)
        id = r.json()['id']
        msg = track_statement_progress(host, id)        
        
        #spark-submit --class pe.com.belcorp.recomender.main.generateParquet s3://belc-bigdata-functional-dlk-qas/analitico/jars/recommender-assembly-0.1.jar -            -path-s3 "s3#://belc-bigdata-apps-shared-qas/recommender" --iteration 1 --country "PE" --campania "201815"         
                

        #s3 = s3fs.S3FileSystem(anon=False, key='', secret='')
        #myopen = s3.open
        #write('belc-bigdata-apps-shared-qas/recommender/output/', df_output_ofertas_parquet, file_scheme='hive', partition_on = ['codpais', 'aniocampana'],#              open_with=myopen) 
        
        if msg == "Livy execution success" : 
          print("entro")
          cursor.execute("update dbo.ARP_Configuracion_Ejecucion_Campaign set parquet=1 where codpais='" + codpais + "' and aniocampanaexpo='" + aniocampana + "' and                            estado=3 and iteracion='" + iteracion + "'")
        
          cursor.execute("update dbo.Arp_Output_Campaign set procesado=1 where codpais='" + codpais + "' and aniocampana='" + aniocampana + "'")
          print("termino")
except pyodbc.Error as e:
    conn.rollback()    
    print(e)
else:
    conn.commit()
    cursor.close()
    conn.close()    

