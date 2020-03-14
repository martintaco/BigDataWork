import boto3
import pandas as pd
import sys
import json
import pymongo

if sys.version_info[0] < 3:
    from StringIO import StringIO # Python 2.x
else:
    from io import StringIO # Python 3.x

S3_ACCESS_KEY= ''
S3_SECRET_KEY= ''
S3_BUCKET_NAME_LOADED= 'belc-bigdata-landing-dlk-qas'
S3_PATH_FILES= 'forecast-data/Bigdata/NovoApp/TacticaInd/'
#S3_BUCKET_NAME_DESTINATION= sys.argv[5]
#S3_PATH_LOAD_DESTINATION = sys.argv[6]

s3 = boto3.client('s3',
                      aws_access_key_id= S3_ACCESS_KEY,
                      aws_secret_access_key= S3_SECRET_KEY
                      )

for obj in s3.list_objects(Bucket = S3_BUCKET_NAME_LOADED, Prefix = S3_PATH_FILES)['Contents']:

    #if obj['Key'] != "forecast-data/Mongo/dmatrizcampana/":
    if obj['Key'] != "forecast-data/Bigdata/NovoApp/TacticaInd/":

        file = s3.get_object(Bucket = S3_BUCKET_NAME_LOADED, Key = obj['Key'])

        file_body = file['Body']

        csv_string = file_body.read().decode('utf-8')
        #Se transforma el csv o txt a dataframe con dtype tipo string y se agrega los nombres de las columnas
        df = pd.read_csv(StringIO(csv_string), sep='\t',dtype = str,header = 0, names = ['codpais','desunidadnegocio','aniomarketing','aniocampana',
                                                                  'campania','codmarca','desmarca','codcategoria',
                                                                  'clase_ajustada','codclase','desclase','tipoajustado',
                                                                  'codtipo','destipo','cuc','descuc','codsap','desproducto',
                                                                  'bpcs','tipomediodeventaajustado','tipomediodeventa',
                                                                  'codgrupooferta','grupoofertaajustado','grupoofertacosmeticos',
                                                                  'codtipooferta','precionormalmn','preciooferta',
                                                                  'precionormaldol','precioofertadol','uudemandadas',
                                                                  'uurealvendidas','estuuvendidas','ventanetamndemandada',
                                                                  'vtarealmnneto','estvtamnneto','ventanetadoldemd',
                                                                  'ventanetadolreal','estvtadolneto','costoreposiciondolreal',
                                                                  'costoreposiciondolest','costodereposicionuntdolreal',
                                                                  'costodereposicionuntdolest','realnropedidos','estnropedidos',
                                                                  'realtcpromedio','submarcas','desproductosupergenerico',
                                                                  'ventanetadolrealcte','etiquetadetopsellers'])
        #Aquí se quitan los valores null o Na del dataframe
        df.fillna('',inplace = True)

#Aquí se hace la conexión con Mongo
#myclient = pymongo.MongoClient("")
#para QAS
#myclient = pymongo.MongoClient("")
#Para PRD
myclient = pymongo.MongoClient("")
mydb = myclient["Tactica"]
mycol = mydb["Individual"]

payload = json.loads(df.to_json(orient='records'))

#se agregan 2 filtros en variables para hacer el borrado por pais diferenciando a PR
#myquery = {"aniocampana": {"$gte":"201904"},"codpais": {"$eq":"PR"}}
#myquery2 = {"aniocampana": {"$gte":"201905"},"codpais": {"$ne":"PR"} }
mycol.remove()
#Y = mycol.delete_many(myquery2)
#X = mycol.delete_many(myquery)
#print(X.deleted_count, " documents deleted.")
#print(Y.deleted_count, " documents deleted.")

#se hace el insert de todo el dataframe evitando los errores que podrian saltar del mongo con el check_keys
mycol.insert(payload,check_keys = False)
