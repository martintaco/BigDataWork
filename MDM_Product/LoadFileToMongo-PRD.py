import boto3
import pandas as pd
import sys
import json
import pymongo

if sys.version_info[0] < 3:
    from StringIO import StringIO # Python 2.x
else:
    from io import StringIO # Python 3.x

if(len(sys.argv) != 10):
  sys.exit("Error: Wrong number of arguments.\nUsage: python (thisFile).py dbhost dbport dbname user password filespath filename")


S3_ACCESS_KEY= sys.argv[1]
S3_SECRET_KEY= sys.argv[2]
S3_BUCKET_NAME_LOADED= sys.argv[3]
S3_PATH_FILES= sys.argv[4]
URL_MONGO_CONNECTION = sys.argv[5]
DB_MONGO = sys.argv[6]
COLLECTION_MONGO = sys.argv[7]
ANIOCAMPANA1= sys.argv[8]
ANIOCAMPANA2 = sys.argv[9]

s3 = boto3.client('s3',
                      aws_access_key_id= S3_ACCESS_KEY,
                      aws_secret_access_key= S3_SECRET_KEY
                      )

for obj in s3.list_objects(Bucket = S3_BUCKET_NAME_LOADED, Prefix = S3_PATH_FILES)['Contents']:

    if obj['Key'] != S3_PATH_FILES:

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
myclient = pymongo.MongoClient(URL_MONGO_CONNECTION)
mydb = myclient[DB_MONGO]
mycol = mydb[COLLECTION_MONGO]

payload = json.loads(df.to_json(orient='records'))

#se agregan 2 filtros en variables para hacer el borrado por pais diferenciando a PR
myquery = {"aniocampana": {"$gte":ANIOCAMPANA1},"codpais": {"$eq":"PR"}}
myquery2 = {"aniocampana": {"$gte":ANIOCAMPANA2},"codpais": {"$ne":"PR"} }
#mycol.remove()
Y = mycol.delete_many(myquery2)
X = mycol.delete_many(myquery)
print(X.deleted_count, " documents deleted.")
print(Y.deleted_count, " documents deleted.")

#se hace el insert de todo el dataframe evitando los errores que podrian saltar del mongo con el check_keys
mycol.insert(payload,check_keys = False)
