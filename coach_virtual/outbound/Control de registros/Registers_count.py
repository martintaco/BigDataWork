import json
import boto3
import pandas as pd
from io import BytesIO
import gzip
import re
import sys
import subprocess
from datetime import datetime, timedelta

if sys.version_info[0] < 3:
    from StringIO import StringIO # Python 2.x
else:
    from io import StringIO # Python 3.x

# Parametros de Conexion
S3_ACCESS_KEY= sys.argv[1]
S3_SECRET_KEY= sys.argv[2]
S3_BUCKET_NAME_LOADED= 'belc-bigdata-landing-dlk-prd'
S3_PATH_FILES= 'lan-virtualcoach/input/data-hybris/'

#parte de nombre de archivo
dia = timedelta(days=1)
start_time = datetime.now()
if start_time.month < 10:
	str_month = "0"+str((start_time-dia).month)
else:
	str_month = str((start_time-dia).month)

if start_time.day < 10:
	str_day = "0"+str((start_time-dia).day)
else:
	str_day = str((start_time-dia).day)
str_year = str(start_time.year)
zipFile = str_year+"-"+str_month+"-"+str_day+".json.gz"
origin_zip_file = "s3://belc-bigdata-landing-dlk-prd/lan-virtualcoach/input/data-hybris/"+zipFile
dest_zip_file = "s3://belc-bigdata-landing-dlk-prd/lan-virtualcoach/input/Register/"
remove_zip_file = "s3://belc-bigdata-landing-dlk-prd/lan-virtualcoach/input/Register/"+zipFile
#Conexion con el bucket de S3
s3 = boto3.client('s3',
                      aws_access_key_id= S3_ACCESS_KEY,
                      aws_secret_access_key= S3_SECRET_KEY
                      )

# move zip to backup
cp_zip_command = ["aws", "s3", "cp", origin_zip_file, dest_zip_file]
subprocess.run(cp_zip_command)
print('the zip file has been copied it into Register')

# Definicion de variables
n = []
listfiles= []
NameFiles = []
# listado de archivos en el bucket
for obj in s3.list_objects(Bucket = S3_BUCKET_NAME_LOADED, Prefix = S3_PATH_FILES)['Contents']:
    listfiles.append(obj['Key'])

for files in listfiles:
    try:
        session = boto3.Session(
        aws_access_key_id= S3_ACCESS_KEY,
        aws_secret_access_key= S3_SECRET_KEY)
        s3 = session.resource('s3')
        key= files #'lan-virtualcoach/input/data-hybris/2019-07-06.json.gz'
        s = key
        #filtro de solo archivos que están luego de los caracteres "data-hybris/"
        ss = re.findall('data-hybris/(.*)', s)
        for i in ss:
            #print(i, end="")
            data = i
            NameFiles.append(data)
            #print(data)
        obj = s3.Object(S3_BUCKET_NAME_LOADED, key)
        dato = obj.get()['Body'].read()
        gzipfile = BytesIO(dato)
        gzipfile = gzip.GzipFile(fileobj=gzipfile)
        content = gzipfile.read()
        json_str = content.decode("utf-8")
        #data = json.loads(json_str)
        number = json_str.count('\n')
        n.append(number)
        #print(n)
        #print(NameFiles)
    except Exception as e:
        print(e)
        #raise e
#remove file from register
rm_zip_command = ["aws", "s3", "rm", remove_zip_file]
subprocess.run(rm_zip_command)
print('the zip file has been removed it from Register')

# Unir ambas listas de registros y files
d =  {'Files':NameFiles[1:],'Registers':n[1:]}
# Transformar a dataframe
df = pd.DataFrame(d)
#imprimir el dataframe como csv
df.to_csv('Reporte_Registros.csv', sep='\t', encoding='utf-8', index=False)
#print
print(df)
