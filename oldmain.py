import os
import time
import numpy as np
import pandas as pd

from flask import Flask, flash, request, redirect, render_template
from werkzeug.utils import secure_filename
from flask_cors import CORS




app = Flask(__name__)
CORS(app)
app.secret_key = "cs provisioning" # for encrypting the session

app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024


path = os.getcwd()
# file Upload
UPLOAD_FOLDER = os.path.join(path, 'uploads')

if not os.path.isdir(UPLOAD_FOLDER):
    os.mkdir(UPLOAD_FOLDER)

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER


ALLOWED_EXTENSIONS = set(['csv','png'])


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@app.route('/')
def upload_form():
    return render_template('index.html')


@app.route('/', methods=['POST'])
def upload_file():
    if request.method == 'POST':
        # check if the post request has the file part
        if 'fname' not in request.files:
            flash(request.files)
            return redirect(request.url)
        file = request.files['fname']
        if file.filename == '':
            flash('No file selected for uploading')
            return redirect(request.url)
        if file and allowed_file(file.filename):
            curFile = secure_filename(file.filename)
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], curFile))
            flash(curFile + ' Uploaded!')
            return redirect(f"/update?fileName={curFile}")
        else:
            flash('Allowed file types are csv')
            return redirect(request.url)


@app.route('/update', methods=['GET','POST'])
def update_spreadsheet():
    if request.method == 'POST':
        if request.form['submit_button'] == "Cancel":
            return 'Cancelling'
        else:
            return 'We did it'
    else:
        return render_template("updateProvision.html",fileName = request.args['fileName'] )


def main_function ():
    if os.path.exists('CareStudioExpansion.csv'):
        old_file= pd.read_csv('CareStudioExpansion.csv')
        timestamp = time.strftime('%H%M-%Y%m%d')
        os.rename('oldname.txt', 'oldname_%s.txt' % (timestamp))

    users_to_provision = pd.read_csv('Care Studio_all users-07122022 - SearchReport.csv')
    initial_input = users_to_provision[['Username','Email','Type','Network (Logon) ID','Job Family','NPI Number','Ministry']]
    output_provision_table = initial_input.rename(columns={'Username': 'NTAccountName','Type':'symphonyemployeetype','Network (Logon) ID':'USERNAME','Job Family':'Cerner Role','NPI Number':'NPI'})
    output_provision_table['PeopleSoft Type'] = output_provision_table['symphonyemployeetype']
    output_provision_table.index = np.arange(1, len(output_provision_table) + 1)
    output_provision_table['Unnamed: 0'] = output_provision_table.index
    output_provision_table[['employeenumber','Care Studio Role']] = ""
    print (output_provision_table[['Unnamed: 0','Email','employeenumber','symphonyemployeetype','PeopleSoft Type','USERNAME','Cerner Role','Care Studio Role','NPI','Ministry']])
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)