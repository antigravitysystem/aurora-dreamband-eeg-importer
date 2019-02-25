from aurora import aurora
import os
import json
from flask import Flask, flash, request, redirect, url_for, render_template, Response
from werkzeug.utils import secure_filename
from db import db

UPLOAD_FOLDER = './sessions'
ALLOWED_EXTENSIONS = set(['zip','csv', 'txt', 'json'])
app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

def allowed_file(filename):
    return '.' in filename and \
        filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

importer = aurora.SESSION_IMPORTER()

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        # check if the post request has the file part
        if 'file' not in request.files:
            flash('No file part')
            return redirect(request.url)
        file = request.files['file']
        # if user does not select file, browser also
        # submit an empty part without filename
        if file.filename == '':
            flash('No selected file')
            return redirect(request.url)
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))

            importer.run_import()

            return redirect(url_for('index', filename=filename))
    return render_template('index.html', title='Welcome')

@app.route('/query', methods=['GET'])
def query():

    query = "SELECT * from sessions"
    record = db.db_query(query)

    return Response(json.dumps(str(record), indent=2), mimetype="text/plain")

try:
    app.run(host="0.0.0.0")

except KeyboardInterrupt:
    importer.db_close()
    print ("Quit")
