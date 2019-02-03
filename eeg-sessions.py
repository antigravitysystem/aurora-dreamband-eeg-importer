#//bin/python

import os
import sys
import json
import pytz
import zipfile
import psycopg2
import shutil
from os import listdir
from os.path import isfile, join, splitext, exists
from pprint import pprint
from datetime import datetime

SESSIONS_DIR = "./sessions"
TMP_PATH = join(SESSIONS_DIR, 'tmp')
IMOPORT_JSON = "import.json"
TIME_ZONE = "Asia/Taipei"

def timestamp_to_datetime(timestamp):
	"""
	The Aurora sessions.json uses longer timestamp digits (13 digits).
	We will check and transform it to datetime format in order to store in DB
	"""

	# check timestamp length if it's longer format
	if(len(str(timestamp))==13):
		timestamp = timestamp/1000

	timezone = pytz.timezone(TIME_ZONE)
	dt = datetime.utcfromtimestamp(timestamp)
	local_dt = timezone.localize(dt)
	tsdt = local_dt.isoformat()
	return tsdt

# check and create folder
if not exists(SESSIONS_DIR):
	os.makedirs(SESSIONS_DIR)

if not exists(TMP_PATH):
	os.makedirs(TMP_PATH)

# look for files in session directory
onlyfiles = [f for f in listdir(SESSIONS_DIR) if isfile(join(SESSIONS_DIR, f))]

has_session_files = False

for file in onlyfiles:
	if file.endswith('.zip'):
		file_path = join(SESSIONS_DIR, file)
		dir_name = splitext(file)[0]
		tmp_session_dir = join(TMP_PATH, dir_name)

		# test the zip file and extract it
		try:
			with zipfile.ZipFile(file_path) as zip_ref:
				print "Extracting", file
				zip_ref.extractall(tmp_session_dir)

				# has_session_files = True
		except zipfile.BadZipfile:
			print "Bad zip! Skip..." + file + "\n"
		print 'delete zip', file_path
		os.remove(file_path)


DB_NAME = 'eeg'
DB_USER = 'postgres'
DB_PASS = 'password'
DB_HOST = '127.0.0.1'
DB_PORT = '5432'

conn = psycopg2.connect(database=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT)
cur = conn.cursor()

# sys.exit()

dirs = listdir(TMP_PATH)

for dir in dirs:

	sub_dir_path = join(TMP_PATH, dir)

	# every session will always contain a session.json file
	session_file_path = join(TMP_PATH, dir, 'session.json')

	# print session_file_path
	if exists(session_file_path):

		has_session_files = True

		# print "session_file_path:", session_file_path
		print "Session :", dir

		# open session.txt, look for string "date: xxxx-xx-xx"
		with open(session_file_path, 'r') as data_file:

			data = json.load(data_file)

			session_id = data['id']
			session_at = data['session_at']/1000
			session_at_dt = timestamp_to_datetime(data['session_at'])
			awake_at = data['awake_at']
			awake_at_dt = timestamp_to_datetime(data['awake_at'])

			query = "CREATE TABLE IF NOT EXISTS sessions (id uuid, session_at timestamptz, awake_at timestamptz);"
			query += "INSERT INTO sessions (id, session_at, awake_at) SELECT '" + session_id + "','" + str(session_at_dt) + "','" + str(awake_at_dt) + "' WHERE NOT EXISTS (SELECT id FROM sessions WHERE id='" + session_id + "');";
			cur.execute(query)
			conn.commit()

		print "Session ID: " + session_id
		print "Session At: " + session_at_dt

		# json file defined the database table name and the datastream file name
		# and interval which we want to import to database later
		with open(IMOPORT_JSON, "r") as file:
			db_import_files = json.load(file)

		stream_files = listdir(sub_dir_path)

		# check out the target files
		for import_file in db_import_files:

			table = import_file['table']
			stream_file = import_file['file']

			# if the csv file exists, which mean it matches the one we setup in import.json.
			# We can start to process the data
			if stream_file in stream_files:

				print "Import to table:", table
				stream_file_path = join(sub_dir_path, stream_file)

				with open(stream_file_path, 'r') as file:
					data = file.read()

				data_list = data.split(",");
				# tsdt = timestamp_to_datetime(session_at)

				timestamp = session_at

				data_list_index = 0

				# since the data stream is pretty huge, we don't want to use all of the data.
				# So we can reduce the size of the stream.
				# The value means to downsize how many times of the data feed.
				down_size = 30

				# caculate the correct interval if we downsize the data stream
				interval = down_size * import_file['interval']

				data_type = import_file['data_type']

				query = "CREATE TABLE IF NOT EXISTS " + table + " (ts timestamptz, value " + data_type + ");"

				for value in data_list:

					# generate the query list, if the record exists, skip it.
					if data_list_index % down_size == 0:
						tsdt = timestamp_to_datetime(timestamp)
						timestamp = timestamp + interval
				 		query = query + "INSERT INTO " + table + " (ts, value) SELECT '" + tsdt + "','" + str(value) + "' WHERE NOT EXISTS (SELECT ts FROM " + table + " WHERE ts='" + tsdt + "');";
					data_list_index += 1

				print "Proccessing", stream_file + "\n"

				cur.execute(query)
				conn.commit()
		print 'delete tmp', sub_dir_path
		shutil.rmtree(sub_dir_path)


# if no files available, just stop
if not has_session_files:
	print 'No file exists! Please put session zip files under \"sessions\" folder. EXIT!'
	sys.exit()


conn.close()
