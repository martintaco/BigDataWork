import boto3
import pandas as pd
import sys
import json
import pymongo
import time

if sys.version_info[0] < 3:
    from StringIO import StringIO # Python 2.x
else:
    from io import StringIO # Python 3.x

S3_ACCESS_KEY= sys.argv[1]
S3_SECRET_KEY= sys.argv[2]
S3_BUCKET_NAME_LOADED= 'belc-bigdata-landing-dlk-prd'
S3_PATH_FILES= 'migracion/Bigdata/NovoApp/TacticaInd/'

ANIOCAMPANA_1= sys.argv[3]
ANIOCAMPANA_2= sys.argv[4]



s3 = boto3.client('s3',
                      aws_access_key_id= S3_ACCESS_KEY,
                      aws_secret_access_key= S3_SECRET_KEY
                      )

for obj in s3.list_objects(Bucket = S3_BUCKET_NAME_LOADED, Prefix = S3_PATH_FILES)['Contents']:

    #if obj['Key'] != "forecast-data/Mongo/dmatrizcampana/":
    if obj['Key'] != S3_PATH_FILES:

        file = s3.get_object(Bucket = S3_BUCKET_NAME_LOADED, Key = obj['Key'])

        file_body = file['Body']

        csv_string = file_body.read().decode('utf-8')
        #Se transforma el csv o txt a dataframe con dtype tipo string y se agrega los nombres de las columnas
        df = pd.read_csv(StringIO(csv_string), sep='\t',dtype = str,header = 0, names = ['codpais','desunidadnegocio','aniomarketing',
								  'aniocampana',
                                                                  'campania','codmarca','desmarca','codcategoria',
                                                                  'clase_ajustada','codclase','codsubcategoria','desclase','tipoajustado',
                                                                  'codtipo','destipo','cuc','descuc','codsap','desproducto',
                                                                  'bpcs','bpcstono',
                                                                  'codgrupooferta','grupoofertaajustado','grupoofertacosmeticos',
                                                                  'codtipooferta','precionormalmn','preciooferta',
                                                                  'precionormaldol','precioofertadol','uudemandadas',
                                                                  'uurealvendidas','estuuvendidas','ventanetamndemandada',
                                                                  'vtarealmnneto','estvtamnneto','ventanetadoldemd',
                                                                  'ventanetadolreal','estvtadolneto','costoreposiciondolreal',
                                                                  'costoreposiciondolest','costodereposicionuntdolreal',
                                                                  'costodereposicionuntdolest','realnropedidos','estnropedidos',
                                                                  'realtcpromedio','submarcas','desproductosupergenerico',
                                                                  'ventanetadolrealcte','etiquetadetopsellers','actividad'])
        #Aquí se quitan los valores null o Na del dataframe
        df.fillna('',inplace = True)

#Aquí se hace la conexión con Mongo
#myclient = pymongo.MongoClient("mongodb+srv://adminBDInfoServiceQAS:FtzDmZ1hLx2yAFsU@bigdatainfoservice-0hd3l.mongodb.net/admin")
#Para PRD
myclient = pymongo.MongoClient("mongodb+srv://adminBDInfoServicePRD:9F1MWjK4Ktdzywk9@bigdatainfoservice-8x89f.mongodb.net/admin")
mydb = myclient["Tactica"]
mycol = mydb["Individual"]


payload = json.loads(df.to_json(orient='records'))

#Se agregan 2 filtros en variables para hacer el borrado por pais diferenciando a PR
myquery = {"aniocampana": {"$gte":ANIOCAMPANA_2},"codpais": "PR"}

#espera 30 segundos
time.sleep(30)

myquery2 = {"aniocampana": {"$gte":ANIOCAMPANA_1},"codpais": {"$ne":"PR"}}
print(myquery2)
print(myquery)
X = mycol.delete_many(myquery)
Y = mycol.delete_many(myquery2)
print(X.deleted_count, " documents deleted.")
print(Y.deleted_count, " documents deleted.")
#Este paso de remove se activa cuando los filtros estan desactivados y se va a eliminar la collection y crear otra con el mismo nombre
#mycol.remove()

#se hace el insert de todo el dataframe evitando los errores que podrian saltar del mongo con el check_keys
mycol.insert(payload,check_keys = False)
