import psycopg2
import datetime
import sys
import boto3

## Autor: Martin Taco

## Uso: python (thisFile).py hostDBRS puertoBDRS nombreBD usuarioRS contrasenhaRS rutaAbsolutaDelArchivo archivoSQL
## Observaciones: El archivo SQL debe estar libre de comentarios (-- o /*) para una ejecucion correcta.
##                De existir algun error en el procedimiento a ejecutar no se commitearan los cambios realizados en queries
##                previas del archivo SQL.

## Tildes omitidas.

start_time = datetime.datetime.now()
print("Inicio : ", start_time.isoformat())

if start_time.day < 10:
	str_day = "0"+str(start_time.day)
else:
	str_day = str(start_time.day)

if start_time.month < 10:
	str_month = "0"+str(start_time.month)
else:
	str_month = str(start_time.month)

tables = ('webredes','comunicaciones','sap')

ACCESS_KEY = sys.argv[1]
SECRET_KEY = sys.argv[2]

## Ejecucion del procedimiento ##
for table in tables:
    if str(table) == 'comunicaciones':
        try:
            client = boto3.resource('s3', aws_access_key_id= ACCESS_KEY, aws_secret_access_key= SECRET_KEY)

            client.Object('belc-bigdata-landing-dlk-prd','datalake/input/mdm/Backup/'+str(table)+'/MDM-Atributos-Comunicaciones-'+str(start_time.year)+str_month+str_day+'.csv').copy_from(CopySource='belc-bigdata-landing-dlk-prd/datalake/input/mdm/'+str(table)+'/MDM-Atributos-Comunicaciones-'+str(start_time.year)+str_month+str_day+'.csv')
            client.Object('belc-bigdata-landing-dlk-prd','datalake/input/mdm/'+str(table)+'/MDM-Atributos-Comunicaciones-'+str(start_time.year)+str_month+str_day+'.csv').delete()

        except Exception as error:
            print('Error: Archivo o carpeta no existen')
            
    elif str(table) == 'webredes':
        try:
            client = boto3.resource('s3', aws_access_key_id= ACCESS_KEY, aws_secret_access_key= SECRET_KEY)

            client.Object('belc-bigdata-landing-dlk-prd','datalake/input/mdm/Backup/'+str(table)+'/MDM-Atributos-WebRedes-'+str(start_time.year)+str_month+str_day+'.csv').copy_from(CopySource='belc-bigdata-landing-dlk-prd/datalake/input/mdm/'+str(table)+'/MDM-Atributos-WebRedes-'+str(start_time.year)+str_month+str_day+'.csv')
            client.Object('belc-bigdata-landing-dlk-prd','datalake/input/mdm/'+str(table)+'/MDM-Atributos-WebRedes-'+str(start_time.year)+str_month+str_day+'.csv').delete()
        
        except Exception as error:
            print('Error: Archivo o carpeta no existen')
    else:
        try:
            client = boto3.resource('s3', aws_access_key_id= ACCESS_KEY, aws_secret_access_key= SECRET_KEY)

            client.Object('belc-bigdata-landing-dlk-prd','datalake/input/mdm/Backup/'+str(table)+'/MDM-Atributos-SAP-'+str(start_time.year)+str_month+str_day+'.csv').copy_from(CopySource='belc-bigdata-landing-dlk-prd/datalake/input/mdm/'+str(table)+'/MDM-Atributos-SAP-'+str(start_time.year)+str_month+str_day+'.csv')
            client.Object('belc-bigdata-landing-dlk-prd','datalake/input/mdm/'+str(table)+'/MDM-Atributos-SAP-'+str(start_time.year)+str_month+str_day+'.csv').delete()

        except Exception as error:
            print('Error: Archivo o carpeta no existen')

end_time = datetime.datetime.now()
print("Fin: ", end_time.isoformat())
