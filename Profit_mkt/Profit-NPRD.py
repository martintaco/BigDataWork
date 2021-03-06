import psycopg2
import datetime
import sys
import boto3

## Autor: David Ascencios

## Uso: python (thisFile).py hostDBRS puertoBDRS nombreBD usuarioRS contrasenhaRS rutaAbsolutaDelArchivo archivoSQL
## Observaciones: El archivo SQL debe estar libre de comentarios (-- o /*) para una ejecucion correcta.
##                De existir algun error en el procedimiento a ejecutar no se commitearan los cambios realizados en queries
##                previas del archivo SQL.

## Tildes omitidas.

start_time = datetime.datetime.now()
print("Inicio : ", start_time.isoformat())

## Verificar argumentos pasados ##
#if(len(sys.argv) != 0):
#  sys.exit("Error: Wrong number of arguments.\nUsage: python (thisFile).py dbhost dbport dbname user password filespath filename")

## Conexion a RS ##
con=psycopg2.connect(host = sys.argv[1],
                     port = sys.argv[2],
                     dbname = sys.argv[3],
                     user = sys.argv[4],
                     password = sys.argv[5])

cursor = con.cursor()

## Agregando una variable más para conocer qué archivo se debe eliminar del S3 ##
filename2 = sys.argv[6]
filename3 = sys.argv[7]
ACCESS_KEY = sys.argv[8]
SECRET_KEY = sys.argv[9]


## Ejecucion del procedimiento ##
try:

    cursor.execute("Select top 0 * into TEMPORARY temp_table from fnc_analitico.dwh_profit_mkt; "+
                   "copy temp_table from 's3://belc-bigdata-landing-dlk-prd/datalake/input/sap-centro/"+filename3+"/"+filename2+"' access_key_id '"+ACCESS_KEY+"' secret_access_key '"+SECRET_KEY+"' delimiter '\t' emptyasnull NULL AS 'NULL' ACCEPTINVCHARS;"+
                   "delete fnc_analitico.dwh_profit_mkt where tipoprofit || codpais || aniocampana || codmarca || codunidadnegocio || codnegocio || codversion in (select distinct tipoprofit || codpais || aniocampana || codmarca || codunidadnegocio || codnegocio || codversion from temp_table); "+
                   "Insert into fnc_analitico.dwh_profit_mkt select * from temp_table;"+" DROP TABLE IF EXISTS temp_table;")
    
    #client = boto3.resource('s3', aws_access_key_id='AKIAJK6A3CSH7NDH2TWA', aws_secret_access_key='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')
    #client.Object('belc-bigdata-landing-dlk-prd','datalake/input/sap-centro/'+ filename3 +'/backup/'+ filename2).copy_from(CopySource='belc-bigdata-landing-dlk-prd/datalake/input/sap-centro/' + filename3 + '/' + filename2)
    #client.Object('belc-bigdata-landing-dlk-prd','datalake/input/sap-centro/'+ filename3 +'/'+ filename2).delete()
    
except psycopg2.InternalError as error:
    # Error code 8001 comes from COPY an empty folder.
    #if not '8001' in repr(error.pgerror):
    sys.exit('0: ' + repr(error.pgerror))
        
except Exception as error:
    sys.exit('4: ' + repr(error))
    
con.commit()
    
end_time = datetime.datetime.now()
print("Fin: ", end_time.isoformat())