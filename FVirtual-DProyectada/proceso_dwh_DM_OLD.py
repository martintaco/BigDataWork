
# coding: utf-8

# In[15]:

import psycopg2
import time
import sys
import datetime

start_time = datetime.datetime.now()
print("Inicio : ", start_time.isoformat())

## Verificar argumentos pasados ##
if(len(sys.argv) != 11):
  sys.exit("Error: Wrong number of arguments.\nUsage: python (thisFile).py dbhost dbport dbname user password s3AccessKey s3SecretKey s3Bucket s3Route")

######FUNCIONES####

def CalculaAnioCampana(anioCampana, delta):
    resultado = str(int(anioCampana)).strip()
    numero = int(resultado[:4])*18 + int(resultado[-2:]) + delta
    anio = str(numero//18)
    campana =str(numero%18).zfill(2)
    resultado = anio + campana
    return resultado

	#executor python3
	#python3 

# In[66]:

con=psycopg2.connect(host = sys.argv[1],
                     port = sys.argv[2],
                     dbname = sys.argv[3],
                     user = sys.argv[4],
                     password = sys.argv[5])

cursor = con.cursor()

## Almacenamiento del contenido del archivo en variable y obtencion del nombre de archivo ##
sqlfile = open(sys.argv[8]+'/'+sys.argv[9],'r')
basename,_=sys.argv[9].split('.')
filename = sys.argv[10]

# In[ ]:

# Truncate lan_dm_analitico tables tables ##
stgTables = ['tmp_demandaproyectada']


for table in stgTables:
    try:
        cursor.execute("TRUNCATE TABLE lan_dm_analitico." + table + ";")
    except Exception as error:
        sys.exit(repr(error))

# In [ ]:

## Copy from S3 To lan_dm_analitico tables
for table in stgTables:
    try:		
        cursor.execute("copy lan_dm_analitico." + table + " from 's3://belc-bigdata-landing-dlk-prd/datalake/input/sap/DemandaProyectada/" + filename + "' access_key_id '" + sys.argv[6] + "' secret_access_key '" + sys.argv[7] + "' ACCEPTANYDATE DATEFORMAT 'auto' ACCEPTINVCHARS IGNOREHEADER AS 1 delimiter ',' region 'us-east-1' escape;")
    except psycopg2.InternalError as error:
        # Error code 8001 comes from COPY an empty folder.
        #if not '8001' in repr(error.pgerror):
        sys.exit('0: ' + repr(error.pgerror))

try:
	cursor.execute(sqlfile.read())
	cursor.execute("COMMIT;")
	
except Exception as error:
    sys.exit(basename +': ' + repr(error))
finally:
    sqlfile.close()

con.commit()

end_time = datetime.datetime.now()
print("Fin: ", end_time.isoformat())