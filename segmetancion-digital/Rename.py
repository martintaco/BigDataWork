import psycopg2
import datetime
import sys
import boto3
import os

## Autor: David Ascencios

ACCESS_KEY = os.environ['S3_ACCESS_KEY']
SECRET_KEY = os.environ['S3_SECRET_KEY']

start_time = datetime.datetime.now()
print("Inicio : ", start_time.isoformat())

## Ejecucion del procedimiento ##

client = boto3.resource('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)	

client.Object('belc-bigdata-domain-dlk-prd','dom-hana/segmentacion_digital/cierre.txt').copy_from(CopySource='belc-bigdata-domain-dlk-prd/dom-hana/segmentacion_digital/cierre000')
	
client.Object('belc-bigdata-domain-dlk-prd','dom-hana/segmentacion_digital/cierre000').delete()

end_time = datetime.datetime.now()
print("Fin: ", end_time.isoformat())
