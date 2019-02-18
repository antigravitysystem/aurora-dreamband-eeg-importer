#//bin/python

import os
import sys
import json
import pytz
import zipfile
import psycopg2
import shutil
import configparser
from os import listdir
from os.path import isfile, join, splitext, exists
from pprint import pprint
from datetime import datetime

class SESSION_IMPORTER():
    def __init__(self):
        self.SESSIONS_DIR = "./sessions"
        self.TMP_PATH = join(self.SESSIONS_DIR, 'tmp')

        # json file defined the database table name and the datastream file name
        # and interval which we want to import to database later
        self.IMOPORT_JSON = "import.json"
        self.TIME_ZONE = "Asia/Taipei"
        self.CONFIG_FILE = 'db.conf'

        # since the data stream is pretty huge, we don't want to use all of the data.
        # So we can reduce the size of the stream.
        # The value means to downsize how many times of the data feed.
        self.DOWN_SIZE = 30

        # check and create folder
        if not exists(self.SESSIONS_DIR):
            print "Create \"sessions\" folder"
            os.makedirs(self.SESSIONS_DIR)

        if not exists(self.TMP_PATH):
            os.makedirs(self.TMP_PATH)

        self.db_connect()

    def timestamp_to_datetime(self, timestamp):
        """
        The Aurora sessions.json uses longer timestamp digits (13 digits).
        We will check and transform it to datetime format in order to store in DB
        """

        # check timestamp length if it's longer format
        if(len(str(timestamp)) == 13):
            timestamp = timestamp/1000

        timezone = pytz.timezone(self.TIME_ZONE)
        dt = datetime.utcfromtimestamp(timestamp)
        local_dt = timezone.localize(dt)
        tsdt = local_dt.isoformat()
        return tsdt

    def extract_files(self):
        SESSIONS_DIR = self.SESSIONS_DIR

        # look for files in session directory
        onlyfiles = [f for f in listdir(SESSIONS_DIR) if isfile(join(SESSIONS_DIR, f))]

        #has_session_files = False

        for file in onlyfiles:
            if file.endswith('.zip'):
                file_path = join(SESSIONS_DIR, file)
                dir_name = splitext(file)[0]
                tmp_session_dir = join(self.TMP_PATH, dir_name)

                # test the zip file and extract it
                try:
                    with zipfile.ZipFile(file_path) as zip_ref:
                        print "Extracting", file
                        zip_ref.extractall(tmp_session_dir)

                # has_session_files = True
                except zipfile.BadZipfile:
                    print "Bad zip! Skip..." + file + "\n"
                    # print 'delete zip', file_path
                os.remove(file_path)

    def read_config(self, config_file):

        config = configparser.ConfigParser()
        config.read(config_file)
        # sections = config.sections()
        if config.has_section('postgresql'):
            return self.read_config_postgresql_settings(config)

    def read_config_postgresql_settings(self, config):
        postgresql = config['postgresql']
        DB_NAME = postgresql['DB_NAME']
        DB_USER = postgresql['DB_USER']
        DB_PASS = postgresql['DB_PASS']
        DB_HOST = postgresql['DB_HOST']
        DB_PORT = postgresql['DB_PORT']
        return DB_NAME, DB_USER, DB_PASS, DB_HOST, DB_PORT


    def db_connect(self):
        config_file = self.CONFIG_FILE
        DB_NAME, DB_USER, DB_PASS, DB_HOST, DB_PORT = self.read_config(config_file)

        try:
            self.conn = psycopg2.connect(database=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT)
            self.cur = self.conn.cursor()
            print "Database connected"
        except:
            print "Database connection failed. EXIT"
            sys.exit()

    def proccess_session_file(self, session_file_path):
                		# open session.txt, look for string "date: xxxx-xx-xx"
        with open(session_file_path, 'r') as data_file:

            data = json.load(data_file)

            session_id = data['id']
            session_at = data['session_at']/1000
            session_at_dt = self.timestamp_to_datetime(data['session_at'])
            awake_at = data['awake_at']
            awake_at_dt = self.timestamp_to_datetime(data['awake_at'])

            query = "CREATE TABLE IF NOT EXISTS sessions (id uuid, session_at timestamptz, awake_at timestamptz);"
            query += "INSERT INTO sessions (id, session_at, awake_at) SELECT '" + session_id + "','" + str(session_at_dt) + "','" + str(awake_at_dt) + "' WHERE NOT EXISTS (SELECT id FROM sessions WHERE id='" + session_id + "');";
            print "Session ID: " + session_id
            print "Session At: " + session_at_dt
            self.cur.execute(query)
            self.conn.commit()

            return session_at

    def import_files_to_db(self, sub_dir_path, session_at):
        import_json = self.IMOPORT_JSON
        with open(import_json, "r") as file:
            db_import_files = json.load(file)

        # check out the target files
        for import_file in db_import_files:

            stream_file = import_file['file']
            stream_files = listdir(sub_dir_path)

            # if the csv file exists, which mean it matches the one we setup in import.json.
            # We can start to process the data
            if stream_file in stream_files:

                table = import_file['table']
                print "Import to table:", table

                stream_file_path = join(sub_dir_path, stream_file)

                with open(stream_file_path, 'r') as file:
                    data = file.read()

                data_list = data.split(",");
                timestamp = session_at
                data_list_index = 0
                down_size = self.DOWN_SIZE

                # caculate the correct interval if we downsize the data stream
                interval = down_size * import_file['interval']

                data_type = import_file['data_type']

                # Create table if not exists
                query = "CREATE TABLE IF NOT EXISTS " + table + " (ts timestamptz, value " + data_type + ");"

                for value in data_list:
                    # generate the query list, if the record exists, skip it.
                    if data_list_index % down_size == 0:
                        tsdt = self.timestamp_to_datetime(timestamp)
                        timestamp = timestamp + interval
                        query += "INSERT INTO " + table + " (ts, value) SELECT '" + tsdt + "','" + str(value) + "' WHERE NOT EXISTS (SELECT ts FROM " + table + " WHERE ts='" + tsdt + "');";
                    data_list_index += 1

                print "Proccessing", stream_file + "\n"

                self.cur.execute(query)
                self.conn.commit()

# sys.exit()
    def run_import(self):
        TMP_PATH = self.TMP_PATH

        self.extract_files()
        dirs = listdir(TMP_PATH)

        print dirs
        for dir in dirs:
            sub_dir_path = join(TMP_PATH, dir)

            # every session will always contain a session.json file
            session_file_path = join(TMP_PATH, dir, 'session.json')

            # print session_file_path
            if exists(session_file_path):

                #has_session_files = True

                # print "session_file_path:", session_file_path
                print "Session :", dir

                session_at = self.proccess_session_file(session_file_path)
                self.import_files_to_db(sub_dir_path, session_at)

                print 'delete tmp', sub_dir_path
                shutil.rmtree(sub_dir_path)

                # if no files available, just stop
            # if not has_session_files:
            else:
                print 'No file exists! Please put session zip files under \"sessions\" folder. EXIT!'
                sys.exit()

        if not self.conn.closed:
            self.conn.close()

if __name__ == '__main__':
    importer = SESSION_IMPORTER()
    importer.run_import()
