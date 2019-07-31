# coding: utf-8

# In[15]:

import psycopg2
import datetime
import time
import sys
from datetime import timedelta

# agregando for para reprocesar 67 campañas:
#for mdays in range(66, 61, -1):
mdays = 65
start_time = (datetime.datetime.now() - timedelta(days=mdays))
##Definiendo variable horarios

# start_time = datetime.datetime.now()
print("Inicio : ", start_time.isoformat())

## Verificar argumentos pasados ##
if (len(sys.argv) != 10):
	sys.exit(
		"Error: Wrong number of arguments.\nUsage: python (thisFile).py dbhost dbport dbname user password s3AccessKey s3SecretKey s3Bucket s3Route")


######FUNCIONES####

def CalculaAnioCampana(anioCampana, delta):
	resultado = str(int(anioCampana)).strip()
	numero = int(resultado[:4]) * 18 + int(resultado[-2:]) + delta
	anio = str(numero // 18)
	campana = str(numero % 18).zfill(2)
	resultado = anio + campana
	return resultado


print("funciones : corrió")
## Declarando lista con todos los paises
paises = ['BO', 'CL', 'CO', 'CR', 'DO', 'EC', 'GT', 'MX', 'PA', 'PE', 'PR', 'SV']

# In[66]:

con = psycopg2.connect(host=sys.argv[1],
					   port=sys.argv[2],
					   dbname=sys.argv[3],
					   user=sys.argv[4],
					   password=sys.argv[5])

cursor = con.cursor()
print("conexiones: corrió")
# In[ ]:

## Truncate lan_dm_analitico tables tables ##
stgTables = ['TMP_ARP_DET_PLANRUTARDD', 'TMP_ARP_DET_CABPLANRUTARDD']

for table in stgTables:
	try:
		cursor.execute("Delete from lan_digital." + table + ";")
	except Exception as error:
		sys.exit(repr(error))
print("Deletes: corrió")
# In [ ]:

## Copy from S3 To lan_dm_analitico tables
for table in stgTables:
	try:
		if start_time.month < 10:
			str_moth = "0" + str(start_time.month)
		else:
			str_moth = str(start_time.month)

		if start_time.day < 10:
			str_day = "0" + str(start_time.day)
		else:
			str_day = str(start_time.day)             
		for pais in paises:
			cursor.execute("copy lan_digital." + table + " from 's3://" + sys.argv[8] + "/" + sys.argv[9] + "/" + table + "/" + pais + "/" + pais + '_' + str(start_time.year) + str_moth + str_day + '.csv' + "' access_key_id '" + sys.argv[6] + "' secret_access_key '" + sys.argv[7] + "' emptyasnull NULL AS 'NULL' IGNOREHEADER AS 1 REMOVEQUOTES ACCEPTINVCHARS delimiter '\t' region 'us-east-1' escape;")

	except psycopg2.InternalError as error:
	# Error code 8001 comes from COPY an empty folder.
		if not '8001' in repr(error.pgerror):
			sys.exit('0: ' + repr(error.pgerror))

# In[ ]:

##pINT_CORP_DET_PLANRUTARDD
try:
	cursor.execute("select planvisitaID,pais into #TempDelete "
				   "from lan_digital.tmp_arp_det_planrutardd group by planvisitaID,pais;")

	cursor.execute("DELETE FROM dom_digital.det_planrutardd "
				   "where pais || planvisitaID in (select pais || planvisitaID from #TempDelete) ;")

	cursor.execute("INSERT INTO dom_digital.det_planrutardd "
				   "SELECT * FROM lan_digital.tmp_arp_det_planrutardd ;")
	cursor.execute("DROP TABLE #TempDelete;")
except Exception as error:
	sys.exit('4: ' + repr(error))

# In[ ]:

##pINT_CORP_DET_CABPLANRUTARDD
try:
	cursor.execute("select ID,Pais into #TempDelete "
				   "from lan_digital.tmp_arp_det_cabplanrutardd group by ID,Pais;")

	cursor.execute("DELETE FROM dom_digital.det_cabplanrutardd "
				   "where pais || ID in (select pais || ID from #TempDelete) ;")

	cursor.execute("INSERT INTO dom_digital.det_cabplanrutardd "
				   "SELECT * FROM lan_digital.tmp_arp_det_cabplanrutardd ;")
	cursor.execute("DROP TABLE #TempDelete;")
except Exception as error:
	sys.exit('5: ' + repr(error))

con.commit()
end_time = datetime.datetime.now()
print("Fin: ", end_time.isoformat())