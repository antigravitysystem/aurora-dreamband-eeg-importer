from aurora import aurora
import os
from flask import Flask, flash, request, redirect, url_for, render_template
from werkzeug.utils import secure_filename

UPLOAD_FOLDER = './sessions'
ALLOWED_EXTENSIONS = set(['zip','csv', 'txt', 'json'])
app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

def allowed_file(filename):
    return '.' in filename and \
        filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# @app.route('/')
# def index():
#      return render_template('index.html', title='Welcome')

@app.route('/upload', methods=['GET', 'POST'])
def upload_file():
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
            importer = aurora.SESSION_IMPORTER()
            importer.run_import()
            return redirect(url_for('upload_file', filename=filename))
    return render_template('index.html', title='Welcome')

try:
    app.run(host="0.0.0.0")
#     importer = aurora.SESSION_IMPORTER()
#     importer.run_import()
except KeyboardInterrupt:
    print "Quit"
