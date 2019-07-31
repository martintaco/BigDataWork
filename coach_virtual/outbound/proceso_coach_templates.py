import psycopg2
import time
import sys

## Verificar argumentos pasados ##
if(len(sys.argv) != 11):
  sys.exit("Error: Wrong number of arguments.\nUsage: python (thisFile).py dbhost dbport dbname user password s3AccessKey s3SecretKey s3Bucket s3Route s3File")



con=psycopg2.connect(host = sys.argv[1],
                     port = sys.argv[2],
                     dbname = sys.argv[3],
                     user = sys.argv[4],
                     password = sys.argv[5])

cursor = con.cursor()


#SE BORRAN LOS REGISTROS ACTUALES EN LA TABLA

try:
    cursor.execute("TRUNCATE TABLE lan_virtual_coach.fdettemplates;")
except Exception as error:
    sys.exit('1: ' + repr(error))



try:
    cursor.execute("copy lan_virtual_coach.fdettemplates  from 's3://" + sys.argv[8] + "/" + sys.argv[9] + "/" + sys.argv[10] + "' access_key_id '" + sys.argv[6] + "' secret_access_key '" + sys.argv[7] + "' CSV ACCEPTINVCHARS delimiter ',' region 'us-east-1';")
except psycopg2.InternalError as error:
    # Error code 8001 comes from COPY an empty folder.
    if not '8001' in repr(error.pgerror):
        sys.exit('2: ' + repr(error.pgerror))


# borrar el header
try:
    cursor.execute("delete from lan_virtual_coach.fdettemplates where length(aniocampanaexpo) != 6;")
except Exception as error:
    sys.exit('3: ' + repr(error))

# pasar solo los registros correspondientes a la campana que se esta subiendo.

try:
    cursor.execute("DELETE FROM fnc_virtual_coach.fdettemplates "
                  "where aniocampanaexpo = (select max (aniocampanaexpo) from lan_virtual_coach.fdettemplates);")

    cursor.execute("INSERT INTO fnc_virtual_coach.fdettemplates SELECT * FROM lan_virtual_coach.fdettemplates;")  
except Exception as error:
    sys.exit('4: ' + repr(error))
