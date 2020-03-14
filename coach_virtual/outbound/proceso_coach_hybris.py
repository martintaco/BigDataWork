import psycopg2
import sys

#S3 Parametros

access_key= sys.argv[1] #S3_ACCESS_KEY_ID
access_secret_key= sys.argv[2] #S3_SECRET_KEY
s3_bucket = sys.argv[3] #S3_BUCKET_NAME

#RedShigt Parametros

dbname = sys.argv[4] #REDSHIFT_DB_NAME
host = sys.argv[5] #REDSHIFT_HOST
port = sys.argv[6] #REDSHIFT_PORT
user = sys.argv[7] #REDSHIFT_USER
password = sys.argv[8] #REDSHIFT_PASSWORD

con=psycopg2.connect(host = host,
                     port = port,
                     dbname = dbname,
                     user = user,
                     password = password)

tmp_tables = [['lan_virtual_coach.fdethybrysdata', 'lan-virtualcoach/input/dlk-data-hybris/']]

cursor = con.cursor()

def load_tmp_tables(stg_tables):
    """

    """
    for table, s3_path in stg_tables:

        #TRUNCATE a las tablas
        try:
            cursor.execute("DELETE " + table + ";")
            
        except Exception as error:
            sys.exit(repr(error))

        #Insert de datos del dia
        try:
            cursor.execute("copy " + table
			   + " from 's3://" + s3_bucket+ "/" + s3_path
                           + "' access_key_id '" + access_key + "' secret_access_key '" + access_secret_key
                           + "' json 'auto' gzip ACCEPTINVCHARS;")

        except psycopg2.InternalError as error:
            # Error code 8001 comes from COPY an empty folder.

            if not '8001' in repr(error.pgerror):
                sys.exit('0: ' + repr(error.pgerror))



if __name__ == "__main__":

    load_tmp_tables(tmp_tables)
    con.commit()
    con.close()
