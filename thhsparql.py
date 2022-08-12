
import logging
import os.path
from os import path, mkdir
import re
import io
import zipfile
import json
from urllib.parse import urljoin, unquote
import requests

import pyparsing
from datetime import datetime

from flask import Flask, render_template, send_file, redirect, url_for, flash
from flask_login import login_user, LoginManager, UserMixin, login_required, current_user
from flask_wtf.csrf import CSRFProtect
from flask_bootstrap import Bootstrap
from flask_moment import Moment
from flask_wtf import FlaskForm
from wtforms import SubmitField, TextAreaField, StringField, SelectField, BooleanField
from flask_wtf.file import FileField, FileAllowed
from werkzeug.security import generate_password_hash, check_password_hash

from rdflib import Graph, Namespace
from rdflib.namespace import RDF, RDFS, XSD, OWL
from owlrl import RDFS_Semantics, DeductiveClosure, OWLRL_Semantics
import yaml

from utils import export_catalog, ttl2csn, di_json2rdf, history

# STATIC Variables
MAX_HISTORY = 100
REPO = 'repo.ttl'
STORED_QUERY_FILE = 'query.json'
QUERY_HISTORY_FILE = 'query_history.json'
IMPORT_HISTORY_FILE = 'import_history.json'
QUERY_CSN_JSON_FILE = 'ttl2csn_queries.csv'
EXPORTED_CATALOG = 'data/catalog.json'
UPLOAD_FOLDER = 'data/uploads'
DIMD = 'data/dimd.ttl'
MD_API = '/app/datahub-app-metadata/api/v1'
MD_API_RUNTIME = '/app/datahub-app-metadata/api/v1/version'
USERS_SPACE = 'data/users'

# Logging
logging.basicConfig(level=logging.INFO)
configs = dict()
TEST = True
if TEST and path.isfile('data/config.yaml'):
    with open('data/config.yaml') as tc:
        test_config = yaml.safe_load(tc)

# Namespaces
dimd = Namespace("https://www.sap.com/products/data-intelligence#")
instance = None

app = Flask(__name__)
csrf = CSRFProtect(app)
app.config['SECRET_KEY'] = "mySec_Key_be_rational"
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
bootstrap = Bootstrap(app)
moment = Moment(app)
login_manager = LoginManager()
login_manager.init_app(app)


# LOGIN
class User(UserMixin):
    def __init__(self, user_id):
        self.id = user_id

    def verify(self):
        if self.id in configs:
            r = requests.get(urljoin(configs[self.id]['host'], MD_API_RUNTIME),
                             headers={'X-Requested-With': 'XMLHttpRequest'},
                             auth=(configs[self.id]['tenant']+'\\'+configs[self.id]['user'],
                                   configs[self.id]['password']))
            if r.status_code != 200:
                return True
            else:
                return False
        else:
            return False

    def get_id(self):
        return self.id


@login_manager.user_loader
def load_user(user_id):
    user = User(user_id)
    if user.verify():
        return user
    else:
        None


@login_manager.unauthorized_handler
def unauthorized_callback():
    return redirect('/login')


# INIT
def init_user_space(user, host, tenant, password):
    seps = re.match(r'.+vsystem\.ingress\.([\w-]+)\.([\w-]+).+', host)
    user_id = seps.group(1) + '.' + seps.group(2) + '-' + tenant + '-' + user
    user_folder = path.join(USERS_SPACE, user_id)

    configs_file = path.join(user_folder, 'config.yaml')
    if path.isfile(configs_file):
        with open(configs_file, 'r') as uc:
            configs[user_id] = yaml.safe_load(uc)
    else:
        configs[user_id] = {'host': host, 'tenant': tenant, 'user': user, 'password': password, 'imports': []}
        with open(configs_file, 'w') as uc:
            yaml.dump(configs[user_id], uc)

    g = Graph()
    g.bind("dimd", dimd)
    g.bind("xsd", XSD)
    g.bind("rdf", RDF)
    g.bind("rdfs", RDFS)
    g.bind("owl", OWL)
    g.parse(DIMD)

    configs[user_id]['graph'] = g
    if path.isfile(path.join(user_folder, REPO)):
        g.parse(path.join(user_folder, REPO))

    i_file = path.join(user_folder, IMPORT_HISTORY_FILE)
    if path.isfile(i_file):
        configs[user_id]['history_import'] = history.History(filename=i_file)
    else:
        configs[user_id]['history_import'] = history.History()

    q_file = path.join(user_folder, QUERY_HISTORY_FILE)
    if path.isfile(q_file):
        configs[user_id]['history_query'] = history.History(filename=q_file)
    else:
        configs[user_id]['history_query'] = history.History()

    sq_file = path.join(user_folder, STORED_QUERY_FILE)
    if path.isfile(sq_file):
        with open(sq_file, 'r') as fp:
            configs[user_id]['stored_queries'] = json.load(fp)
    else:
        configs[user_id]['stored_queries'] = dict()

    return user_id


def parse_rdf_file(graph, file):
    file_path = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
    file.save(file_path)
    file_extension = os.path.splitext(file.filename)[1].lower()
    logging.info(f'Add: {file.filename}')
    if file_extension == '.rdf':
        graph.parse(file_path, format='xml')
    else:
        graph.parse(file)
    graph.serialize(destination=REPO)


class MainForm(FlaskForm):
    di_host = StringField("URL")
    di_tenant = StringField("Tenant")
    di_user = StringField("User")
    # di_pwd = PasswordField("Password")
    di_pwd = StringField("Password")
    di_connection = StringField("Connection")
    di_container = StringField("Container")
    submit_import_back = SubmitField('\u2190')
    submit_import_forward = SubmitField('\u2192')
    submit_import_new = SubmitField("New")
    submit_import_add = SubmitField("Add")
    file_field_rdf = FileField('', validators=[FileAllowed(['ttl', 'turtle', 'rdf'], 'ttl-only')])
    submit_new = SubmitField('New')
    submit_add = SubmitField('Add')
    submit_save = SubmitField('Save')
    submit_download = SubmitField('Download')
    submit_csn_json = SubmitField('To CSN/JSON')
    selected_query = SelectField('Select Saved Query', choices=['Last Query'])
    submit_use_query = SubmitField('Use Query')
    textarea_cmd = TextAreaField('Sparql Command')
    submit_back = SubmitField('\u2190')
    submit_forward = SubmitField('\u2192')
    submit_save_query = SubmitField('Save Query')
    save_text = StringField('As', default='Last Query', )
    submit_run = SubmitField('Run')
    submit_reasoning = SubmitField('Reasoning')
    check_use_namespaces = BooleanField(label='Use Namespaces: ', description="Use Namespaces", default=True)
    check_unquote = BooleanField(label='Unquote URL: ', description="Unquote URL", default=True)


class LoginForm(FlaskForm):
    di_host = StringField("URL")
    di_tenant = StringField("Tenant")
    di_user = StringField("User")
    # di_pwd = PasswordField("Password")
    di_pwd = StringField("Password")
    submit_login = SubmitField("Login")


def delete_user_space(user_id):
    os.remove(path.join(USERS_SPACE, user_id))
    logging.info(f"User space deleted: {user_id}")


@app.route('/login', methods=['GET', 'POST'])
def login():
    form = LoginForm()
    if TEST:
        form.di_host.data = test_config['host']
        form.di_user.data = test_config['user']
        form.di_tenant.data = test_config['tenant']
        form.di_pwd.data = test_config['password']
    if form.validate_on_submit():
        user_id = init_user_space(user=form.di_user.data, password=form.di_pwd.data,
                                  host=form.di_host.data, tenant=form.di_tenant.data)
        login_user(User(user_id))
        return redirect(url_for('index'))

    return render_template('login.html', form=form)


@app.route('/', methods=['GET', 'POST'])
@login_required
def index():
    global instance
    ui = current_user.id
    g = configs[ui]['graph']
    form = MainForm()
    form.selected_query.choices = list(configs[ui]['stored_queries'].keys())
    form.di_host.data = configs[ui]['host']
    form.di_user.data = configs[ui]['user']
    form.di_tenant.data = configs[ui]['tenant']
    form.di_pwd.data = configs[ui]['password']
    if configs[ui]['history_import'].pointer_value():
        form.di_connection.data, form.di_container.data = configs[ui]['history_import'].pointer_value().split(',')
    else:
        form.di_connection.data, form.di_container.data = "", ""
    status = ""
    if form.validate_on_submit():
        # Buttons: ADD, NEW, SAVE IMPORT
        if form.submit_new.data or form.submit_add.data or form.submit_save.data or form.submit_download.data or \
            form.submit_csn_json.data or form.submit_import_new.data or form.submit_import_add.data or \
                form.submit_import_back.data or form.submit_import_forward.data:
            status = 'RDF repo button pressed.'
            if form.submit_import_back.data:
                logging.info(f"Back import history")
                form.di_connection.data, form.di_container.data = configs[ui]['history_import'].back().split(',')
                return render_template('main.html', form=form,
                                       rdflist_header=['Imports'], rdflist_body=configs['imports'],
                                       result_header=[], result_body=[],
                                       status=f"Forward in import history: {configs[ui]['history_import'].pointer_str()}")
            elif form.submit_import_forward.data:
                logging.info(f"Forward import history")
                form.di_connection.data, form.di_container.data = configs[ui]['history_import'].forward().split(',')
                return render_template('main.html', form=form,
                                       rdflist_header=['Imports'], rdflist_body=configs['imports'],
                                       result_header=[], result_body=[],
                                        status=f"Forward in import history: {configs[ui]['history_import'].pointer_str()}")

            # Import Catalog Container
            elif form.submit_import_new.data or form.submit_import_add.data:
                logging.info("Export Process started")
                render_template('main.html', form=form,
                                rdflist_header=['Imports'], rdflist_body=configs['imports'],
                                result_header=[], result_body=[],
                                status=f"Exporting catalog started ...")
                catalog_data = export_catalog.export_catalog(form.di_host.data, form.di_tenant.data,
                                                             MD_API,
                                                             form.di_user.data, form.di_pwd.data,
                                                             form.di_connection.data, form.di_container.data)
                if len(catalog_data) == 0:
                    logging.warning("No dataset found for {form.di_connection.data} - {form.di_container.data}")
                    status = "No dataset found for {form.di_connection.data} - {form.di_container.data}"
                    return render_template('main.html', form=form,
                                           rdflist_header=['Imports'], rdflist_body=configs[ui]['imports'],
                                           result_header=[], result_body=[],
                                           status=status)

                configs[ui]['history_import'].forward(','.join([form.di_connection.data, form.di_container.data]))

                configs[ui]['host'] = form.di_host.data
                configs[ui]['tenant'] = form.di_tenant.data
                configs[ui]['user'] = form.di_user.data
                configs[ui]['password'] = form.di_pwd.data

                base_url = configs[ui]['host'] + '/' + configs[ui]['tenant'] + '/'
                instance = Namespace(base_url)
                logging.info("RDF Conversion started")
                g_new = di_json2rdf.to_rdf(catalog_data, instance)
                if form.submit_import_new.data:
                    g = Graph()
                    g.parse(DIMD)
                    g = g + g_new
                    configs[ui]['imports'] = [form.di_connection.data + form.di_container.data]
                    configs[ui]['graph'] = g
                else:
                    g = g + g_new
                    configs[ui]['imports'].append(form.di_connection.data + form.di_container.data)
                    configs[ui]['graph'] = g
                with open(path.join(USERS_SPACE, ui, 'config.yaml'), 'w') as cf:
                    yaml.dump(configs[ui], cf)
                configs[ui]['graph'].serialize(destination=REPO)

            elif form.submit_new.data:
                if not form.file_field_rdf.data:
                    status = "Select file first!"
                else:
                    filename = form.file_field_rdf.data.filename
                    logging.info(f'New: {filename}')
                    g = Graph()
                    g.parse(DIMD)
                    g.bind("dimd", dimd)
                    parse_rdf_file(g, form.file_field_rdf.data)
                    configs['imports'] = [filename]
                    configs[ui]['graph'] = g
                    with open(path.join(USERS_SPACE, ui, 'config.yaml'), 'w') as cf:
                        yaml.dump(configs[ui], cf)
                    status = f"New RDF graph: {filename}. Query history deleted. "
            elif form.submit_add.data:
                if not form.file_field_rdf.data:
                    status = "Select file first!"
                else:
                    filename = form.file_field_rdf.data.filename
                    parse_rdf_file(g, form.file_field_rdf.data)
                    configs[ui]['imports'].append(filename)
                    with open(path.join(USERS_SPACE, ui, 'config.yaml'), 'w') as cf:
                        yaml.dump(configs[ui], cf)
                    status = f"Added RDF graph: {filename}"
            elif form.submit_save.data:
                configs[ui]['graph'].serialize(destination=REPO)
                status = f"Saved graph to repo!"
            elif form.submit_download.data:
                configs[ui]['graph'].serialize(destination=REPO)
                logging.info(f"Downloaded graph")
                graph_io = io.BytesIO()
                with zipfile.ZipFile(graph_io, mode='w') as z:
                    z.write(path.join(USERS_SPACE, ui, REPO))
                graph_io.seek(0)
                return send_file(graph_io, as_attachment=True, download_name='repo.zip', mimetype='application/zip')
            elif form.submit_csn_json.data:
                name = os.path.splitext(configs[ui]['imports'])[0]
                csn_json = ttl2csn.ttl2json(configs[ui]['graph'], path.join(USERS_SPACE, ui, QUERY_CSN_JSON_FILE), name)
                filename = os.path.join(path.join(USERS_SPACE, ui, name + '_ER_Model.json'))
                with open(filename, mode='w') as js:
                    js.write(csn_json)
                logging.info(f"Download converted ER-model (json): {filename}")
                return send_file(filename, as_attachment=True, download_name=os.path.basename(filename))
            else:
                submit_filter = form.submit_new.data or form.submit_add.data or form.submit_save.data or \
                                form.submit_download.data or form.submit_csn_json.data
                logging.error(f"The filter and the selection does not match? {submit_filter}")
                raise ValueError(f"The filter and the selection does not match? {submit_filter}")

            return render_template('main.html', form=form,
                                   rdflist_header=['Imports'], rdflist_body=configs[ui]['imports'],
                                   result_header=[], result_body=[],
                                   status=status)

    # Backward query history
    if form.submit_back.data:
        logging.info(f"Backward query history")
        form.textarea_cmd.data = configs[ui]['history_query'].back()
        return render_template('main.html', form=form,
                               rdflist_header=['Imports'], rdflist_body=configs[ui]['imports'],
                               result_header=[], result_body=[],
                               status=f"Back in query history: {configs[ui]['history_query'].pointer_str()}")
    # forward query history
    elif form.submit_forward.data:
        logging.info(f"Forward query history")
        form.textarea_cmd.data = configs[ui]['history_query'].forward()
        return render_template('main.html', form=form,
                               rdflist_header=['Imports'], rdflist_body=configs[ui]['imports'],
                               result_header=[], result_body=[],
                               status=f"Forward in query history: {configs[ui]['history_query'].pointer_str()}")
    # Copy Query
    elif form.submit_use_query.data:
        logging.info(f"Copy query to SPARQL-field")
        form.textarea_cmd.data = configs[ui]['stored_queries'][form.selected_query.data]
        return render_template('main.html', form=form,
                               rdflist_header=['Imports'], rdflist_body=configs[ui]['imports'],
                               result_header=[], result_body=[],
                               status='Copied query')
    # Run Query
    elif form.submit_run.data:
        logging.info(f'Query: {form.textarea_cmd.data}')
        configs[ui]['history_query'].append(str(form.textarea_cmd.data))
        try:
            render_template('main.html', form=form,
                            rdflist_header=['Imports'], rdflist_body=configs[ui]['imports'],
                            result_header=[], result_body=[],
                            status='Query running...')
            start_time = datetime.now()
            statement = form.textarea_cmd.data
            logging.info(f"Query: {statement}")
            if re.match(r'\s*INSERT\s+.+', statement):
                query_results = configs[ui]['graph'].update(statement)
                query_statement = False
            elif re.match(r'\s*SELECT\s+.+', statement):
                query_results = configs[ui]['graph'].query(statement)
                query_statement = True
            else:
                logging.error(f'Unknown query? {statement}')
                raise pyparsing.exceptions.ParseException(f'Unknown query (not implemented)?')
            run_time = (datetime.now() - start_time).total_seconds()
        except Exception as pe:
            logging.error(pe)
            return render_template('main.html', form=form,
                                   rdflist_header=['Imports'], rdflist_body=configs[ui]['imports'],
                                   result_header=[], result_body=[],
                                   status=f"Parsing error: {pe}")
        else:
            if query_statement:
                if len(query_results.vars) > 1:
                    if form.check_use_namespaces.data:
                        if form.check_unquote.data:
                            result_body = [
                                [unquote(r[v].n3(configs[ui]['graphs'].namespace_manager)) for v in query_results.vars
                                 if r[v]] for r in query_results]
                        else:
                            result_body = [
                                [r[v].n3(configs[ui]['graphs'].namespace_manager) for v in query_results.vars
                                 if r[v]] for r in query_results]

                    else:
                        if form.check_unquote.data:
                            result_body = [[unquote(r[v]) for v in query_results.vars if r[v]] for r in query_results]
                        else:
                            result_body = [[r[v] for v in query_results.vars if r[v]] for r in query_results]
                else:
                    var = query_results.vars[0]
                    if form.check_use_namespaces.data:
                        if form.check_unquote.data:
                            result_body = [[unquote(i)] for i in set([r[var].n3(configs[ui]['graphs'].namespace_manager)
                                                                      for r in query_results])]
                        else:
                            result_body = [[i] for i in set([r[var].n3(configs[ui]['graphs'].namespace_manager)
                                                             for r in query_results])]
                    else:
                        if form.check_unquote.data:
                            result_body = [unquote(i) for i in set([r[var] for r in query_results])]
                        else:
                            result_body = [i for i in set([r[var] for r in query_results])]

                return render_template('main.html', form=form,
                                       rdflist_header=['Imports'], rdflist_body=configs[ui]['imports'],
                                       result_header=query_results.vars, result_body=result_body,
                                       status=f'Query runtime: {run_time}')
            else:
                return render_template('main.html', form=form,
                                       rdflist_header=['Imports'], rdflist_body=configs[ui]['imports'],
                                       result_header=[], result_body=[],
                                       status=f'Insert runtime: {run_time}')

    # Save Query
    elif form.submit_save_query.data:
        logging.info(f'Query saved: {form.textarea_cmd.data}')
        configs[ui]['stored_queries'][form.save_text.data] = form.textarea_cmd.data
        with open(path.join(USERS_SPACE, ui, STORED_QUERY_FILE, 'w')) as qr:
            json.dump(configs[ui]['stored_queries'], qr, indent=4)

    elif form.submit_reasoning.data:
        logging.info(f'Start Deductive Closure ("RDFS_Semantics","OWLRL_Semantics")')
        DeductiveClosure(RDFS_Semantics).expand(configs[ui]['graphs'])
        DeductiveClosure(OWLRL_Semantics).expand(configs[ui]['graphs'])

    return render_template('main.html', form=form,
                           rdflist_header=['Imports'], rdflist_body=configs[ui]['imports'],
                           result_header=[], result_body=[],
                           status=status)


if __name__ == '__main__':
    app.run('0.0.0.0', port=5000)
