import psycopg2
import pandas as pd
import sys

REDSHIFT_HOST = sys.argv[1]
REDSHIFT_PORT = sys.argv[2]
REDSHIFT_DB = sys.argv[3]
REDSHIFT_USER = sys.argv[4]
REDSHIFT_PASSWORD = sys.argv[5]
ACCESS_KEY_ID= sys.argv[6]
SECRET_KEY_ID= sys.argv[7]

con=psycopg2.connect(host = REDSHIFT_HOST,
                     port = REDSHIFT_PORT,
                     dbname = REDSHIFT_DB,
                     user = REDSHIFT_USER,
                     password = REDSHIFT_PASSWORD)


cursor = con.cursor()

#Para ejecutar desde el nifi
#python3 TacticaIndividual.py 10.12.2.26 5439 analitico usr_app B3Icorp8457
df = pd.read_sql_query("""
Delete sbx_temp.temp_Data_generate3;
select codpais, f_calculaaniocampana(codpais,max(aniocampana),-1) as aniocampana_ini,
f_calculaaniocampana(codpais,max(aniocampana),0) as aniocampana_fin,
f_calculaaniocampana(codpais,MAX(aniocampana),1) as aniocampana_ini2,
f_calculaaniocampana(codpais,MAX(aniocampana),10) as aniocampana_fin2
from fnc_analitico.dwh_fstaebecam
where codpais in ('BO','PR')
group by codpais
order by 1;""",con)

##Nombre_Archivo=archivo.replace (ruta,"")

for index, row in df.iterrows():

    sql = open(str('TacticaIndividual.sql'), 'r').read()
    sql = sql.format(CodPais=row['codpais'], Aniocampana_ini=row['aniocampana_ini'], Aniocampana_fin=row['aniocampana_fin'], Aniocampana_ini2=row['aniocampana_ini2'], Aniocampana_fin2=row['aniocampana_fin2'],ACCESS_KEY=ACCESS_KEY_ID,SECRET_KEY=SECRET_KEY_ID)
    cursor.execute(sql)
    con.commit()
    #print(row['codpais'], row['aniocampana_ini'],row['aniocampana_fin'],row['aniocampana_ini2'],row['aniocampana_fin2'])
    print(row['codpais'], row['aniocampana_ini'])