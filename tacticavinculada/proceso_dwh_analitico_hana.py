# coding: utf-8

# In[15]:

import psycopg2
import time
import sys

## Verificar argumentos pasados ##
if(len(sys.argv) != 10):
  sys.exit("Error: Wrong number of arguments.\nUsage: python (thisFile).py dbhost dbport dbname user password s3AccessKey s3SecretKey s3Bucket s3Route")

######FUNCIONES####

def CalculaAnioCampana(anioCampana, delta):
    resultado = str(int(anioCampana)).strip()
    numero = int(resultado[:4])*18 + int(resultado[-2:]) + delta
    anio = str(numero//18)
    campana =str(numero%18).zfill(2)
    resultado = anio + campana
    return resultado


# In[66]:

con=psycopg2.connect(host = sys.argv[1],
                     port = sys.argv[2],
                     dbname = sys.argv[3],
                     user = sys.argv[4],
                     password = sys.argv[5])

cursor = con.cursor()

# In[ ]:

## Truncate lan_dm_analitico tables tables ##
stgTables = ['TMP_ARP_CORP_DET_TACTICA_CONDICION', 'TMP_ARP_CORP_DET_TACTICA_OFERTA', 'TMP_ARP_CORP_MAE_TACTICA_CONDICION',  		 		
			 'TMP_ARP_CORP_MAE_TACTICA_VINCULADA']

for table in stgTables:
    try:
        cursor.execute("TRUNCATE TABLE lan_dm_analitico." + table + ";")
    except Exception as error:
        sys.exit(repr(error))

# In [ ]:

## Copy from S3 To lan_dm_analitico tables
for table in stgTables:
    try:
        cursor.execute("copy lan_dm_analitico." + table + " from 's3://" + sys.argv[8] + "/" + sys.argv[9] + "/" + table + "/" + table + "' access_key_id '" + sys.argv[6] + "' secret_access_key '" + sys.argv[7] + "' GZIP REMOVEQUOTES ACCEPTINVCHARS delimiter '\t' region 'us-east-1' escape;")
    except psycopg2.InternalError as error:
        # Error code 8001 comes from COPY an empty folder.
        if not '8001' in repr(error.pgerror):
            sys.exit('0: ' + repr(error.pgerror))

# In[ ]:

##pINT_CORP_DET_TACTICA_CONDICION
try:
		cursor.execute("select CodPais,AnioCampana into #TempDelete "
				   "from lan_dm_analitico.tmp_arp_corp_det_tactica_condicion group by CodPais,AnioCampana;" )

		cursor.execute("DELETE FROM fnc_analitico.corp_det_tactica_condicion "
		 #"where exists (select * from #TempDelete b where b.CodPais=CodPais and b.AnioCampana=AnioCampana);")
		 "where codpais || aniocampana in (select codpais||aniocampana from #TempDelete) ;")

		cursor.execute("INSERT INTO fnc_analitico.corp_det_tactica_condicion "
				"SELECT * FROM lan_dm_analitico.tmp_arp_corp_det_tactica_condicion ;")
		cursor.execute("DROP TABLE #TempDelete;")
except Exception as error:
			sys.exit('4: ' + repr(error))
			
# In[ ]:

##pINT_CORP_DET_TACTICA_OFERTA
try:
		cursor.execute("select CodPais,AnioCampana into #TempDelete "
				   "from lan_dm_analitico.tmp_arp_corp_det_tactica_oferta group by CodPais,AnioCampana;" )

		cursor.execute("DELETE FROM fnc_analitico.corp_det_tactica_oferta "
		 #"where exists (select * from #TempDelete b where b.CodPais=CodPais and b.AnioCampana=AnioCampana);")
		 "where codpais || aniocampana in (select codpais||aniocampana from #TempDelete) ;")

		cursor.execute("INSERT INTO fnc_analitico.corp_det_tactica_oferta "
				"SELECT * FROM lan_dm_analitico.tmp_arp_corp_det_tactica_oferta ;")
		cursor.execute("DROP TABLE #TempDelete;")
except Exception as error:
			sys.exit('4: ' + repr(error))			

# In[ ]:

##pINT_CORP_MAE_TACTICA_CONDICION
try:
		cursor.execute("select CodPais,AnioCampana into #TempDelete "
				   "from lan_dm_analitico.tmp_arp_corp_mae_tactica_condicion group by CodPais,AnioCampana;" )

		cursor.execute("DELETE FROM fnc_analitico.corp_mae_tactica_condicion "
		 #"where exists (select * from #TempDelete b where b.CodPais=CodPais and b.AnioCampana=AnioCampana);")
		 "where codpais || aniocampana in (select codpais||aniocampana from #TempDelete) ;")

		cursor.execute("INSERT INTO fnc_analitico.corp_mae_tactica_condicion "
				"SELECT * FROM lan_dm_analitico.tmp_arp_corp_mae_tactica_condicion ;")
		cursor.execute("DROP TABLE #TempDelete;")
except Exception as error:
			sys.exit('4: ' + repr(error))			

# In[ ]:

##pINT_CORP_MAE_TACTICA_VINCULADA
try:
		cursor.execute("select CodPais,AnioCampana into #TempDelete "
				   "from lan_dm_analitico.tmp_arp_corp_mae_tactica_vinculada group by CodPais,AnioCampana;" )

		cursor.execute("DELETE FROM fnc_analitico.corp_mae_tactica_vinculada "
		 #"where exists (select * from #TempDelete b where b.CodPais=CodPais and b.AnioCampana=AnioCampana);")
		 "where codpais || aniocampana in (select codpais||aniocampana from #TempDelete) ;")

		cursor.execute("INSERT INTO fnc_analitico.corp_mae_tactica_vinculada "
				"SELECT * FROM lan_dm_analitico.tmp_arp_corp_mae_tactica_vinculada ;")
		cursor.execute("DROP TABLE #TempDelete;")
except Exception as error:
			sys.exit('4: ' + repr(error))			
			
