import json
import boto3
import pandas as pd
from io import BytesIO
import gzip
import re
import sys

if sys.version_info[0] < 3:
    from StringIO import StringIO # Python 2.x
else:
    from io import StringIO # Python 3.x

# Parametros de Conexion
S3_ACCESS_KEY= sys.argv[1]
S3_SECRET_KEY= sys.argv[2]
S3_BUCKET_NAME_LOADED= 'belc-bigdata-landing-dlk-prd'
S3_PATH_FILES= 'lan-virtualcoach/input/data-hybris/'

#Conexion con el bucket de S3
s3 = boto3.client('s3',
                      aws_access_key_id= S3_ACCESS_KEY,
                      aws_secret_access_key= S3_SECRET_KEY
                      )
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
# Unir ambas listas de registros y files
d =  {'Files':NameFiles[1:],'Registers':n[1:]}
# Transformar a dataframe
df = pd.DataFrame(d)
#imprimir el dataframe como csv
df.to_csv('Reporte_Registros.csv', sep='\t', encoding='utf-8', index=False)
#print
print(df)
