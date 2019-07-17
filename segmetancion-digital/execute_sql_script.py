import psycopg2
import time
import sys

## Verificar argumentos pasados ##
if(len(sys.argv) < 8):
  sys.exit("Error: Wrong number of arguments.\nUsage: python (thisFile).py dbhost dbport dbname user password filespath filename")



con=psycopg2.connect(host = sys.argv[1],
                     port = sys.argv[2],
                     dbname = sys.argv[3],
                     user = sys.argv[4],
                     password = sys.argv[5])


cursor = con.cursor()
sqlfile = open(sys.argv[6]+'/'+sys.argv[7],'r')
basename,_=sys.argv[7].split('.')

try:
    sql = sqlfile.read()
    if (len(sys.argv)>8): # Is there any args to the sql file?
        sql = sql % tuple(sys.argv[8:len(sys.argv)])        
    cursor.execute(sql)
    cursor.execute("COMMIT;")
except Exception as error:
    sys.exit(basename +': ' + repr(error))
finally:
    sqlfile.close()
