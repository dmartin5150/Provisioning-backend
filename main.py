# from collections import _OrderedDictValuesView
from hashlib import new
from operator import ne
import os
from webbrowser import get
from matplotlib import use
from datetime import datetime as dt, timedelta 
import numpy as np
import pandas as pd
import shutil
from flask import Flask, flash, request, redirect, render_template, send_from_directory,abort
from psutil import users
from werkzeug.utils import secure_filename
from flask_cors import CORS

UPLOADED_COLUMNS = ['Email','Job Family','Ministry','Network (Logon) ID','NPI Number','Type','Username']
PROVISIONING_WORKSHEETS = ['All Users', 'New Users', 'Deprovisioned Users', 'Source File']

app = Flask(__name__)
CORS(app)
app.secret_key = "cs provisioning" # for encrypting the session

app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024


path = os.getcwd()
UPLOAD_FOLDER = os.path.join(path, 'uploads')
PRIOR_UPLOAD_FOLDER = os.path.join(UPLOAD_FOLDER,'prior')
UPDATED_FILES_FOLDER = os.path.join(path,'UpdatedFiles')
PRIOR_UPDATED_FILES_FOLDER = os.path.join(UPDATED_FILES_FOLDER,'prior')

if not os.path.isdir(UPLOAD_FOLDER):
    os.mkdir(UPLOAD_FOLDER)

if not os.path.isdir(UPDATED_FILES_FOLDER):
    os.mkdir(UPDATED_FILES_FOLDER)

if not os.path.isdir(PRIOR_UPLOAD_FOLDER):
    os.mkdir(PRIOR_UPLOAD_FOLDER)


app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['UPDATED_FILES_FOLDER'] = UPDATED_FILES_FOLDER
app.config['PRIOR_UPLOAD_FOLDER'] = PRIOR_UPLOAD_FOLDER
app.config['PRIOR_UPDATED_FILES_FOLDER'] = PRIOR_UPDATED_FILES_FOLDER

ALLOWED_EXTENSIONS = set(['csv'])


def copy_files(src,dst):
    files = [i for i in os.listdir(src)]
    print('files',files)
    for f in files:
        if (os.path.isfile(os.path.join(src,f))):
            try:
                print('moving: {}'.format(f))
                shutil.copy(os.path.join(src, f), os.path.join(dst,f))
            except OSError as err:
                print("OS error: {0}".format(err))
                return False
    return True



def copy_previous_uploaded_file():
    src = app.config['UPLOAD_FOLDER']
    dst = app.config['PRIOR_UPLOAD_FOLDER']
    if (copy_files(src,dst)):
        return True
    return False

def copy_current_provisioning_files():
    src = app.config['UPDATED_FILES_FOLDER']
    dst = app.config['PRIOR_UPDATED_FILES_FOLDER'] 
    if (copy_files(src,dst)):
        return True
    return False

def get_uploaded_file_data(request):
    uploaded_file = request.files['file_from_react'];
    uploaded_filename = secure_filename(uploaded_file.filename)
    file_data = {
        'filename': uploaded_filename,
        'file': uploaded_file
    }
    return file_data


def save_uploaded_file(file_data):
  try: 
    file_data['file'].save(os.path.join(app.config['UPLOAD_FOLDER'], file_data['filename']))
    return create_response('Got file!', 200)
  except OSError as err:
    print("OS error: {0}".format(err))
    return create_response('Unable to upload file', 500)



@app.route('/upload', methods=['POST'])
def upload_file():
    file_data= get_uploaded_file_data(request)
    if copy_previous_uploaded_file():
        response, status = save_uploaded_file(file_data)
        return response, status
    else:
        return create_response('Unable to move uploaded file!', 500) 

def provisioning_csv_exsits():
    try :
        if os.path.exists(os.path.join(UPDATED_FILES_FOLDER, 'CSProvisioning.csv')):
            return create_response('CSProvisioning.csv exisits', 200)
        else:
            return create_response('CSProvisioning.csv does not exist', 404)
    except OSError as err:
        print("OS error: {0}".format(err))
        return create_response('Unable to upload file',500)

def get_filename(folder,filetype):
    src = app.config[folder]
    file = ''
    for fname in os.listdir(src):
        if filetype in fname:
            file = fname 
    print('file ', file)
    return file  

def get_file_creation_date(folder,filename):
    src = app.config[folder]
    path = os.path.join(src, filename)
    creation_time =  int(os.path.getctime(path))
    creation_date = dt.fromtimestamp(creation_time).strftime('%Y-%m-%d %H:%M:%S')
    return creation_date

def get_file_data(folder, filetype):
    filename = get_filename(folder,filetype)
    creation_date = get_file_creation_date(folder,filename)
    file_data = {
        'filename': filename,
        'creation_date': creation_date,
    }
    return file_data

def uploaded_csv_exists():
    filename = get_filename('UPLOAD_FOLDER','csv')
    return filename != ''


def update_header_columns(dataframe):
    dataframe.columns = dataframe.iloc[0]
    dataframe = dataframe[1:]
    return dataframe

def remove_empty_columns(dataframe):
    updated_dataframe = dataframe.dropna(how='all',axis=1)
    return updated_dataframe

def get_first_users(dataframe, valid_data_index):
    first_users = dataframe.loc[:valid_data_index-1,:] 
    first_users = update_header_columns(first_users)
    first_users = remove_empty_columns(first_users)
    return first_users

def get_last_users(dataframe, valid_data_index):
    last_users = dataframe.loc[valid_data_index:,:]
    last_users = update_header_columns(last_users)
    last_users = remove_empty_columns(last_users)
    return last_users

#Upload file sometimes has a creator header generated when it is built that needs to be removed
def remove_creator_header(dataframe): 
    valid_data_index =  dataframe['Creator'].first_valid_index()
    first_users = get_first_users(dataframe, valid_data_index)
    last_users = get_last_users(dataframe, valid_data_index)
    all_users = pd.concat([first_users, last_users], axis=0)
    all_users.reset_index(drop=True,inplace=True)
    return all_users



def create_response(message, status):
    response = {
        'message': message
    }
    return response, status

#Removes header data that is duplicated throughout the data
def remove_duplicate_headers(dataframe):
    return dataframe.drop_duplicates(keep=False)

def dataframe_has_correct_columns(dataframe):
    columns = dataframe.columns.tolist()
    columns_match = all(item in columns for item in UPLOADED_COLUMNS)
    return (columns_match)

def dataframe_not_empty(dataframe):
    return dataframe.shape[0] != 0


def validate_uploaded_data(uploaded_dataframe):
    if dataframe_not_empty(uploaded_dataframe):
        if dataframe_has_correct_columns(uploaded_dataframe):
            return create_response('Data is valid', 200)
        else:
            return create_response('Uploaded file missing required column!', 400)
    else:
        return create_response('Unable to find uploaded file or it is empty!', 400)


@app.route('/', methods=['GET'])
def upload_form():
    return create_response('Connected!', 200)

def get_dataframe(folder, file_name):
    src = app.config[folder]
    file = os.path.join(src, file_name)
    return pd.read_csv(file)


def get_sourcefile_dataframe():
    sourcefilename = get_filename('UPLOAD_FOLDER','csv')
    sourcefile_data = {'Source File': [sourcefilename]}
    sourcefile_dataframe = pd.DataFrame(sourcefile_data)
    return sourcefile_dataframe



def create_new_provisioning_dataframe(uploaded_dataframe):
    initial_input = uploaded_dataframe[['Username','Email','Type','Network (Logon) ID','Job Family','NPI Number','Ministry']]
    provisioning_dataframe = initial_input.rename(columns={'Username': 'NTAccountName','Type':'symphonyemployeetype','Network (Logon) ID':'USERNAME','Job Family':'Cerner Role','NPI Number':'NPI'})
    provisioning_dataframe['PeopleSoft Type'] = provisioning_dataframe['symphonyemployeetype']
    provisioning_dataframe.index = np.arange(1, len(provisioning_dataframe) + 1)
    provisioning_dataframe['Unnamed: 0'] = provisioning_dataframe.index
    provisioning_dataframe[['employeenumber','Care Studio Role']] = ""
    return provisioning_dataframe[['Unnamed: 0','Email','employeenumber','symphonyemployeetype','PeopleSoft Type','USERNAME','Cerner Role','Care Studio Role','NPI','Ministry','NTAccountName']]

def create_new_provisioning_dataframes(new_dataframe, current_dataframe):
    all_user_dataframe = create_new_provisioning_dataframe(new_dataframe)
    new_user_dataframe = get_new_users(new_dataframe,current_dataframe)
    deprovisioned_user_dataframe = get_deprovisioned_users(new_dataframe, current_dataframe)
    sourcefile_dataframe = get_sourcefile_dataframe()
    provisioning_dataframes = {
        'all': all_user_dataframe,
        'new': new_user_dataframe,
        'deprovision':deprovisioned_user_dataframe,
        'source': sourcefile_dataframe }
    return provisioning_dataframes

def create_blank_dataframe():
    column_names = ['Unnamed: 0','Email','employeenumber','symphonyemployeetype','PeopleSoft Type','USERNAME','Cerner Role','Care Studio Role','NPI','Ministry','NTAccountName']
    return pd.DataFrame(columns=column_names)

def create_new_spreadsheet(user_dataframe):
    current_dataframe = create_blank_dataframe()
    provisioning_dataframes = create_new_provisioning_dataframes(user_dataframe,current_dataframe)
    response = write_new_provisioning_files(provisioning_dataframes)
    return response


# current_dataframe['NTAccountName] = new_dataframe['Username']
def get_deprovisioned_users(new_dataframe,current_dataframe):
    return current_dataframe[~current_dataframe['NTAccountName'].isin(list(new_dataframe['Username']))]

# current_dataframe['NTAccountName] = new_dataframe['Username']
def get_new_users(new_dataframe, current_dataframe):
    return new_dataframe[~new_dataframe['Username'].isin(list(current_dataframe['NTAccountName']))]

def get_current_user_dataframe():
    try: 
        return get_dataframe('UPDATED_FILES_FOLDER','CSProvisioning.csv')
    except OSError as err:
        return pd.DataFrame()

def current_user_dataframe_empty():
    return get_current_user_dataframe().shape == 0

def add_deprovisioned_users(new_user_dataframe,current_user_dataframe):
    deprovisioned_users= get_deprovisioned_users (new_user_dataframe, current_user_dataframe)

def add_newly_provisioned_users(new_user_dataframe, current_user_dataframe):
    new_users = get_new_users(new_user_dataframe,current_user_dataframe)

def current_user_dataframe_is_valid(dataframe):
    return dataframe.shape[0] != 0

def write_provisioning_csv(provisioning_dataframe, filename='CSProvisioning.csv'):
    provisioning_dataframe.to_csv(os.path.join(UPDATED_FILES_FOLDER, 'CSProvisioning.csv'))

def write_provisioning_excel_worksheet(writer, path, provisioning_dataframe, worksheet_name):
    provisioning_dataframe.to_excel(writer, sheet_name=worksheet_name, index=False)

def write_provisioning_excel(provisioning_dataframes, filename='CSProvisioning.xlsx'):
    path = os.path.join(UPDATED_FILES_FOLDER, filename)
    writer = pd.ExcelWriter(path , engine='xlsxwriter')
    for df_index, worksheet in zip(provisioning_dataframes, PROVISIONING_WORKSHEETS):
            write_provisioning_excel_worksheet(writer,path, provisioning_dataframes[df_index], worksheet)
    writer.save()

def write_new_provisioning_files(provisioning_dataframes):
    try:
        write_provisioning_csv(provisioning_dataframes['all'])
        write_provisioning_excel(provisioning_dataframes)
        return create_response('New provisoning spreadsheet created', 200)
    except:
        return create_response('Unable to write files', 500)

def update_blank_data_from_current_spreadsheet(new_dataframe, current_dataframe):
    return new_dataframe.update(current_dataframe,overwrite=False)

def update_exisiting_spreadsheet(new_user_dataframe):
    current_user_dataframe = get_current_user_dataframe()
    if current_user_dataframe_is_valid(current_user_dataframe):
        provisioning_dataframes = create_new_provisioning_dataframes(new_user_dataframe,current_user_dataframe)
        update_blank_data_from_current_spreadsheet(provisioning_dataframes['all'], current_user_dataframe)
        provisioning_dataframes['all'].to_csv(os.path.join(UPDATED_FILES_FOLDER, 'CSProvisioning.csv'))
        response = write_new_provisioning_files(provisioning_dataframes)
        return response
    else:
        return create_response('Unable to access current spreadsheet!', 404)

def server_error(status):
    return status == 500

def file_not_found(status):
    return status == 404

def update_spreadsheet(user_dataframe):
    response, status = provisioning_csv_exsits()
    if server_error(status):
        return create_response('Server error', 500)
    elif file_not_found(status) or current_user_dataframe_empty(): #if no provisining spreadsheet exists, create new one
        response, status = create_new_spreadsheet(user_dataframe)
        return response, status
    else:
        if copy_current_provisioning_files():
            response, status = update_exisiting_spreadsheet(user_dataframe)
            return response, status
        else:
            return create_response('Unable to save current provisioning files', 500)

def uploaded_file_needs_formating(dataframe):
    if 'Creator' in dataframe.columns:
        return True
    return False

def format_uploaded_dataframe(uploaded_dataframe):
    uploaded_dataframe = remove_creator_header(uploaded_dataframe)
    uploaded_dataframe = remove_duplicate_headers(uploaded_dataframe)
    return uploaded_dataframe

def get_uploaded_dataframe():
    if not uploaded_csv_exists():
        return pd.DataFrame()
    uploaded_filename = get_filename('UPLOAD_FOLDER','csv')
    src = app.config['UPLOAD_FOLDER']
    uploaded_file = os.path.join(src, uploaded_filename)
    uploaded_dataframe= pd.read_csv(uploaded_file)

    if uploaded_file_needs_formating(uploaded_dataframe):
        uploaded_dataframe = format_uploaded_dataframe(uploaded_dataframe)
    return uploaded_dataframe

def get_current_sourcefile():
    provisioning_filename = get_filename('UPDATED_FILES_FOLDER','xlsx')
    if provisioning_filename != '':
        excel_file = os.path.join(UPDATED_FILES_FOLDER, provisioning_filename)
        sourcefile_dataframe = pd.read_excel(excel_file, sheet_name='Source File')
        if (sourcefile_dataframe.shape[0] > 0):
            return sourcefile_dataframe['Source File'][0]
    return ''


@app.route('/spreadsheet', methods=['GET'])
def process_spreadsheet_data():
    uploaded_dataframe = get_uploaded_dataframe()
    response, status = validate_uploaded_data(uploaded_dataframe)
    if status == 200:
        response, status = update_spreadsheet(uploaded_dataframe)
        return response, status
    else:
        return response, status

@app.route('/provisioningCSV', methods=['GET'])
def getProvisioningCSV():
    return send_from_directory(UPDATED_FILES_FOLDER,'CSProvisioning.csv',as_attachment=True)

@app.route('/provisioningExcel', methods=['GET'])
def getProvisioningExcel():
    return send_from_directory(UPDATED_FILES_FOLDER,'CSProvisioning.xlsx',as_attachment=True)

@app.route('/currentstatus', methods=['GET'])
def getCurrentStatus():
    print('in current status')
    uploaded_file = get_file_data('UPLOAD_FOLDER','csv')
    updated_provisioning_file = get_file_data('UPDATED_FILES_FOLDER','csv')
    source_file = get_current_sourcefile()
    response = {
        'upload': uploaded_file,
        'provision': updated_provisioning_file,
        'source': source_file,
    }
    print(response)
    return response, 200

app.run(host='0.0.0.0', port=5000)