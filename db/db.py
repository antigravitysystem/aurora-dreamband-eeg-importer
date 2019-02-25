import psycopg2
import configparser
import sys

def read_config(config_file):

    config = configparser.ConfigParser()
    config.read(config_file)
    # sections = config.sections()
    if config.has_section('postgresql'):
        postgresql = config['postgresql']
        DB_NAME = postgresql['DB_NAME']
        DB_USER = postgresql['DB_USER']
        DB_PASS = postgresql['DB_PASS']
        DB_HOST = postgresql['DB_HOST']
        DB_PORT = postgresql['DB_PORT']
        return DB_NAME, DB_USER, DB_PASS, DB_HOST, DB_PORT
        return read_config_postgresql_settings(config)


def db_query(query, **param):

    config_file = 'db.conf'

    DB_NAME, DB_USER, DB_PASS, DB_HOST, DB_PORT = read_config(config_file)

    commit = False
    fetchone = False

    for key, value in param.items():
        if key == 'commit':
            commit = value
        if key == 'fetchone':
            fetchone = value

    try:
        connection = psycopg2.connect(database=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT)
        cursor = connection.cursor()
        # Print PostgreSQL Connection properties
        # print ( connection.get_dsn_parameters(),"\n")
        # Print PostgreSQL version
        cursor.execute(query)

        if commit == True:
            connection.commit()
        else:
            if fetchone == True:
                record = cursor.fetchone()
            else:
                record = cursor.fetchall()

            print("Query result - ", record,"\n")

            return record

    except (Exception, psycopg2.Error) as error :

        print ("Error while connecting to PostgreSQL", error)
    finally:
        #closing database connection.
        if(connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

if __name__ == '__main__':
    query = "SELECT version();"
    db_query(query, fetchone=True)
