from belcorp.aws import Redshift
import sys

params = {
    'AWS_ACCESS_KEY': sys.argv[1],
    'AWS_SECRET_KEY': sys.argv[2]
}

sql_file = 'segmentacion-digital.sql'

redshift = Redshift()
redshift.execute_sql_script(sql_file, params)
redshift.close()
