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

## Declarando lista con todos los paises
paises = ['BO','CL','CO','CR','DO','EC','GT','MX','PA','PE','PR','SV']

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

## Ejecucion del procedimiento ##
try:
	cursor.execute(sqlfile.read())
	cursor.execute("COMMIT;")

	if start_time.month < 10:
		str_month = "0"+str(start_time.month)
	else:
		str_month = str(start_time.month)

	if start_time.day < 10:
		str_day = "0"+str(start_time.day)
	else:
		str_day = str(start_time.day)

	client = boto3.resource('s3', aws_access_key_id= ACCESS_KEY, aws_secret_access_key= SECRET_KEY)

	for pais in paises:
		cursor.execute("UNLOAD ($$ SELECT * FROM dom_digital.det_consultora_sb where codpais = '"+str(pais)+"';$$) TO 's3://belc-bigdata-domain-dlk-prd/dom-digital/rutadesarrollo/"+pais+'/'+pais+'_'+str(start_time.year)+str_month+str_day+"' CREDENTIALS 'aws_access_key_id="+ACCESS_KEY+";aws_secret_access_key="+SECRET_KEY+"' DELIMITER ',' ADDQUOTES NULL AS '' PARALLEL OFF ESCAPE;")
		client.Object('belc-bigdata-domain-dlk-prd','dom-digital/rutadesarrollo/'+pais+'/'+ pais + '_' + str(start_time.year)+str_month + str_day + '.csv').copy_from(CopySource='belc-bigdata-domain-dlk-prd/dom-digital/rutadesarrollo/' + pais + '/' + pais + '_' + str(start_time.year) + str_month + str_day + '000')
		client.Object('belc-bigdata-domain-dlk-prd','dom-digital/rutadesarrollo/'+pais+'/'+ pais + '_' + str(start_time.year) + str_month + str_day + '000').delete()

except Exception as error:
    sys.exit(basename +': ' + repr(error))
finally:
    sqlfile.close()

end_time = datetime.datetime.now()
print("Fin: ", end_time.isoformat())
