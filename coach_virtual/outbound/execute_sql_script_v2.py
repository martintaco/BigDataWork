import psycopg2
import time
import sys
import os

## Verificar argumentos pasados ##
if(len(sys.argv) < 9):
  sys.exit("Error: Wrong number of arguments.\nUsage: python (thisFile).py dbhost dbport dbname user password filespath filename")

con=psycopg2.connect(host = sys.argv[1],
                     port = sys.argv[2],
                     dbname = sys.argv[3],
                     user = sys.argv[4],
                     password = sys.argv[5])


cursor = con.cursor()
sqlfile = open(sys.argv[6]+'/'+sys.argv[7], 'r')
basename,_=sys.argv[7].split('.')

S3_ACCESS_KEY = os.environ['S3_ACCESS_KEY']
S3_SECRET_KEY = os.environ['S3_SECRET_KEY']
codedate = sys.argv[8]

try:
    sql = sqlfile.read()
    sql = sql.format(ACCESS_KEY=S3_ACCESS_KEY,SECRET_KEY=S3_SECRET_KEY,date1 = codedate)
#    if (len(sys.argv)>8 and sys.argv[7] != 'results.sql' and sys.argv[7] != 'Q2.sql' and sys.argv[7] != 'Q2_Manual.sql' and sys.argv[7] != 'results_v2.sql' and sys.argv[7] != 'VC_Resultstableau.sql' ): # Is there any args to the sql file?
#        sql = sql % tuple(sys.argv[8:(len(sys.argv)-2)])        
    cursor.execute(sql)
    cursor.execute("COMMIT;")
except Exception as error:
    sys.exit(basename +': ' + repr(error))
finally:
    sqlfile.close()
