import boto3
import pandas as pd
import sys
import json
import pymongo

if sys.version_info[0] < 3:
    from StringIO import StringIO # Python 2.x
else:
    from io import StringIO # Python 3.x

S3_ACCESS_KEY= 'AKIAJK6A3CSH7NDH2TWA'
S3_SECRET_KEY= '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'
S3_BUCKET_NAME_LOADED= 'belc-bigdata-landing-dlk-prd'
S3_PATH_FILES= 'datalake/input/mdm/dmatrizcampana/'
#S3_BUCKET_NAME_DESTINATION= sys.argv[5]
#S3_PATH_LOAD_DESTINATION = sys.argv[6]

s3 = boto3.client('s3',
                      aws_access_key_id= S3_ACCESS_KEY,
                      aws_secret_access_key= S3_SECRET_KEY
                      )

for obj in s3.list_objects(Bucket = S3_BUCKET_NAME_LOADED, Prefix = S3_PATH_FILES)['Contents']:

    #if obj['Key'] != "forecast-data/Mongo/dmatrizcampana/":
    if obj['Key'] != "datalake/input/mdm/dmatrizcampana/":

        file = s3.get_object(Bucket = S3_BUCKET_NAME_LOADED, Key = obj['Key'])

        file_body = file['Body']

        csv_string = file_body.read().decode('utf-8')
        #Se transforma el csv o txt a dataframe con dtype tipo string y se agrega los nombres de las columnas
        df = pd.read_csv(StringIO(csv_string), sep='\t',dtype = str,header = 0, names = ['aniocampana','codcanalventa','codcatalogo',
                                                                             'codestrategia','codtipooferta','codventa',
                                                                             'descatalogo','destipooferta','nropagina',
                                                                             'numoferta','precionormalmn','preciooferta',
                                                                             'preciovtapropuestomn','codtipocatalogo',
                                                                             'desargventa','desexposicion','desladopag',
                                                                             'destipocatalogo','desubicacioncatalogo',
                                                                             'fotomodelo','fotoproducto','nropaginas',
                                                                             'paginacatalogo','desobservaciones',
                                                                             'vehiculoventa','codpais','codsap',
                                                                             'codtipomedioventa','demandaanormalplan',
                                                                             'desestrategia','destipodiagramacion',
                                                                             'factorrepeticion','flagdiscover',
                                                                             'flagestadisticable','flagproductosebe',
                                                                             'indcuadre','indpadre','precionormaldolplan',
                                                                             'precionormalmnplan','precioofertadolplan',
                                                                             'precioofertamnplan','factorcuadre'])
        #Aquí se quitan los valores null o Na del dataframe
        df.fillna('',inplace = True)

#Aquí se hace la conexión con Mongo
#myclient = pymongo.MongoClient("mongodb+srv://adminJMTM:Martin245522@myclustedbmtm-4wbx0.mongodb.net/admin")
#para QAS
#myclient = pymongo.MongoClient("mongodb+srv://adminBDInfoServiceQAS:FtzDmZ1hLx2yAFsU@bigdatainfoservice-0hd3l.mongodb.net/admin")
#Para PRD
myclient = pymongo.MongoClient("mongodb+srv://userBDInfoServicePRD:7A0BMQ3YHS1gTSg0@bigdatainfoservice-8x89f.mongodb.net/admin")
mydb = myclient["info_product"]
mycol = mydb["dmatrizcampana"]

payload = json.loads(df.to_json(orient='records'))

#se agregan 2 filtros en variables para hacer el borrado por pais diferenciando a PR
myquery = {"aniocampana": {"$gte":"201904"},"codpais": {"$eq":"PR"}}
myquery2 = {"aniocampana": {"$gte":"201906"},"codpais": {"$ne":"PR"} }
#mycol.remove()
Y = mycol.delete_many(myquery2)
X = mycol.delete_many(myquery)
print(X.deleted_count, " documents deleted.")
print(Y.deleted_count, " documents deleted.")

#se hace el insert de todo el dataframe evitando los errores que podrian saltar del mongo con el check_keys
mycol.insert(payload,check_keys = False)
