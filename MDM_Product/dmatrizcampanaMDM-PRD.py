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

## Se extrae la información de la dmatrizcampana y se hace un unload al bucket de s3 en landing Qas y en landing PRD

start_time = datetime.datetime.now()
print("Inicio : ", start_time.isoformat())

## Verificar argumentos pasados ##
#if(len(sys.argv) != 8):
  #sys.exit("Error: Wrong number of arguments.\nUsage: python (thisFile).py dbhost dbport dbname user password filespath filename")

## Conexion a RS ##
con=psycopg2.connect(host = sys.argv[1],
                     port = sys.argv[2],
                     dbname = sys.argv[3],
                     user = sys.argv[4],
                     password = sys.argv[5])

cursor = con.cursor()

## Almacenamiento del contenido del archivo en variable y obtencion del nombre de archivo ##
#sqlfile = open(sys.argv[6]+'/'+sys.argv[7],'r')
#basename,_=sys.argv[7].split('.')
aniocampana1 = sys.argv[6]
aniocampana2 = sys.argv[7]
ACCESS_KEY = sys.argv[8]
SECRET_KEY = sys.argv[9]


## Ejecucion del procedimiento ##
try:
    client = boto3.resource('s3', aws_access_key_id= ACCESS_KEY,
                            aws_secret_access_key= SECRET_KEY)

    cursor.execute("drop table IF EXISTS Temp_data;")
    cursor.execute("drop table IF EXISTS #Temp_data1;")

    #cursor.execute("Select * into Temp_data from fnc_analitico.dwh_dmatrizcampana where codpais != 'PR' and aniocampana >= '" + str(aniocampana1) + "' Union select * from fnc_analitico.dwh_dmatrizcampana where codpais = 'PR' and aniocampana >= '" + str(aniocampana2) + "';")
    cursor.execute("Select * into #Temp_data1 from fnc_analitico.dwh_dmatrizcampana where codpais != 'PR' and aniocampana >= '" + str(aniocampana1) + "' Union select * from fnc_analitico.dwh_dmatrizcampana where codpais = 'PR' and aniocampana >= '" + str(aniocampana2) + "';")
    cursor.execute(
         "UNLOAD ($$ SELECT * FROM #Temp_data1; $$) TO 's3://belc-bigdata-landing-dlk-prd/datalake/input/mdm/dmatrizcampana/dmatrizcampana.csv' CREDENTIALS 'aws_access_key_id="+ ACCESS_KEY+ ";aws_secret_access_key="+SECRET_KEY+"' DELIMITER '\t' HEADER ALLOWOVERWRITE PARALLEL OFF ESCAPE ADDQUOTES;")

    client.Object('belc-bigdata-landing-dlk-prd', 'datalake/input/mdm/dmatrizcampana/dmatrizcampana.csv').copy_from(
        CopySource='belc-bigdata-landing-dlk-prd/datalake/input/mdm/dmatrizcampana/dmatrizcampana.csv000')
    client.Object('belc-bigdata-landing-dlk-prd', 'datalake/input/mdm/dmatrizcampana/dmatrizcampana.csv000').delete()

    cursor.execute(
        "UNLOAD ($$ SELECT * FROM #Temp_data1; $$) TO 's3://belc-bigdata-landing-dlk-qas/forecast-data/Mongo/dmatrizcampana/dmatrizcampana.csv' CREDENTIALS 'aws_access_key_id="+ACCESS_KEY+";aws_secret_access_key="+SECRET_KEY+"' DELIMITER '\t' HEADER ALLOWOVERWRITE PARALLEL OFF ESCAPE ADDQUOTES;")
    client.Object('belc-bigdata-landing-dlk-qas', 'forecast-data/Mongo/dmatrizcampana/dmatrizcampana.csv').copy_from(
        CopySource='belc-bigdata-landing-dlk-qas/forecast-data/Mongo/dmatrizcampana/dmatrizcampana.csv000')
    client.Object('belc-bigdata-landing-dlk-qas', 'forecast-data/Mongo/dmatrizcampana/dmatrizcampana.csv000').delete()

except Exception as error:
    sys.exit('4: ' + repr(error))

end_time = datetime.datetime.now()
print("Fin: ", end_time.isoformat())
