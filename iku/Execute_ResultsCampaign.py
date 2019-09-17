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
if(len(sys.argv) != 10):
  sys.exit("Error: Wrong number of arguments.\nUsage: python (thisFile).py dbhost dbport dbname user password filespath filename")



## Conexion a RS ##
con=psycopg2.connect(host = sys.argv[1],
                     port = sys.argv[2],
                     dbname = sys.argv[3],
                     user = sys.argv[4],
                     password = sys.argv[5])

cursor = con.cursor()

## Almacenamiento del contenido del archivo en variable y obtencion del nombre de archivo ##
sqlfile = open(sys.argv[6]+'/'+sys.argv[7],'r')
basename,_=sys.argv[7].split('.')
ACCESS_KEY = sys.argv[8]
SECRET_KEY = sys.argv[9]
sqlfile = sqlfile.format(ACCESS_KEY=ACCESS_KEY,SECRET_KEY=SECRET_KEY)

## Ejecucion del procedimiento ##
try:
	cursor.execute(sqlfile.read())
	cursor.execute("COMMIT;")

except Exception as error:
    sys.exit(basename +': ' + repr(error))
finally:
    sqlfile.close()

end_time = datetime.datetime.now()
print("Fin: ", end_time.isoformat())
