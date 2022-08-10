
import logging
import os.path
from os import path
import re
import io
import zipfile
import json
import urllib

import pyparsing
from datetime import datetime

from flask import Flask, render_template, send_file
from flask_wtf.csrf import CSRFProtect
from flask_bootstrap import Bootstrap
from flask_moment import Moment
from flask_wtf import FlaskForm
from wtforms import SubmitField, TextAreaField, StringField, SelectField, BooleanField
from flask_wtf.file import FileField, FileAllowed

from rdflib import Graph, Namespace
from rdflib.namespace import RDF, RDFS, XSD, OWL
from owlrl import RDFS_Semantics, DeductiveClosure, OWLRL_Semantics
import yaml

from utils import export_catalog, ttl2csn, di_json2rdf, history

# STATIC Variables
MAX_HISTORY = 100
REPO = 'data/repo.ttl'
CONFIGS = 'data/config.yaml'
QUERY_FILE = 'data/query.json'
QUERY_HISTORY_FILE = 'data/query_history.json'
IMPORT_HISTORY_FILE = 'data/import_history.json'
QUERY_CSN_JSON_FILE = 'data/ttl2csn_queries.csv'
EXPORTED_CATALOG = 'data/catalog.json'
UPLOAD_FOLDER = 'data/uploads'
ALLOWED_EXTENSIONS = {'rdf', 'ttl', 'xml', 'json', 'turtle'}
DIMD = 'dimd.ttl'


app = Flask(__name__)
csrf = CSRFProtect(app)
app.config['SECRET_KEY'] = "mySec_Key_be_rational"
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
bootstrap = Bootstrap(app)
moment = Moment(app)

# Logging
logging.basicConfig(level=logging.DEBUG)


# Global variables
g = Graph()
result_header = list()
result_body = list()
stored_queries = dict()


# Namespaces
dimd = Namespace("https://www.sap.com/products/data-intelligence#")
instance = None

# INIT
if path.exists(REPO):
    g = Graph()
    g.bind("dimd", dimd)
    g.bind("xsd", XSD)
    g.bind("rdf", RDF)
    g.bind("rdfs", RDFS)
    g.bind("owl", OWL)

    g.parse(DIMD)
    g.parse(REPO)

    with open(CONFIGS, 'r') as fp:
        configs = yaml.safe_load(fp)

    history_import = history.History(filename=IMPORT_HISTORY_FILE)
    history_query = history.History(filename=QUERY_HISTORY_FILE)

    with open(QUERY_FILE, 'r') as fp:
        stored_queries = json.load(fp)


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


@app.route('/', methods=['GET', 'POST'])
def index():
    global g, instance, result_body, result_header, history_query, stored_queries, history_import

    form = MainForm()
    form.selected_query.choices = list(stored_queries.keys())
    form.di_host.data = configs['di']['host']
    form.di_user.data = configs['di']['user']
    form.di_tenant.data = configs['di']['tenant']
    form.di_pwd.data = configs['di']['password']
    form.di_connection.data, form.di_container.data = history_import.pointer_value().split(',')
    status = ""
    if form.validate_on_submit():
        # Buttons: ADD, NEW, SAVE IMPORT
        if form.submit_new.data or form.submit_add.data or form.submit_save.data or form.submit_download.data or \
            form.submit_csn_json.data or form.submit_import_new.data or form.submit_import_add.data or \
                form.submit_import_back.data or form.submit_import_forward.data:
            status = 'RDF repo button pressed.'
            if form.submit_import_back.data:
                logging.info(f"Back import history")
                form.di_connection.data, form.di_container.data = history_import.back().split(',')
                return render_template('import_rdf.html', form=form,
                                       rdflist_header=['Imports'], rdflist_body=configs['import_list'],
                                       result_header=[], result_body=[],
                                       status=f'Forward in import history: {history_import.pointer_str()}')
            elif form.submit_import_forward.data:
                logging.info(f"Forward import history")
                form.di_connection.data, form.di_container.data = history_import.forward().split(',')
                return render_template('import_rdf.html', form=form,
                                       rdflist_header=['Imports'], rdflist_body=configs['import_list'],
                                       result_header=[], result_body=[],
                                       status=f'Forward in import history: {history_import.pointer_str()}')

            # Import Catalog Container
            elif form.submit_import_new.data or form.submit_import_add.data:
                logging.info("Export Process started")
                render_template('import_rdf.html', form=form,
                                rdflist_header=['Imports'], rdflist_body=configs['import_list'],
                                result_header=[], result_body=[],
                                status=f"Exporting catalog started ...")
                catalog_data = export_catalog.export_catalog(form.di_host.data, form.di_tenant.data,
                                                             configs['di']['path'],
                                                             form.di_user.data, form.di_pwd.data,
                                                             form.di_connection.data, form.di_container.data)
                if len(catalog_data) == 0:
                    logging.warning("No dataset found for {form.di_connection.data} - {form.di_container.data}")
                    status = "No dataset found for {form.di_connection.data} - {form.di_container.data}"
                    return render_template('import_rdf.html', form=form,
                                           rdflist_header=['Imports'], rdflist_body=configs['import_list'],
                                           result_header=[], result_body=[],
                                           status=status)

                history_import.forward(','.join([form.di_connection.data, form.di_container.data]))

                configs['di']['host'] = form.di_host.data
                configs['di']['tenant'] = form.di_tenant.data
                configs['di']['user'] = form.di_user.data
                configs['di']['password'] = form.di_pwd.data

                base_url = configs['di']['host'] + '/' + configs['di']['tenant'] + '/'
                instance = Namespace(base_url)
                logging.info("RDF Conversion started")
                g_new = di_json2rdf.to_rdf(catalog_data, instance)
                if form.submit_import_new.data:
                    g = Graph()
                    g.parse(DIMD)
                    g = g + g_new
                    configs['import_list'] = [form.di_connection.data + form.di_container.data]
                else:
                    g = g + g_new
                    configs['import_list'].append(form.di_connection.data + form.di_container.data)
                with open(CONFIGS, 'w') as cf:
                    yaml.dump(configs, cf)
                g.serialize(destination=REPO)

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
                    configs['import_list'] = [filename]
                    with open(CONFIGS, 'w') as cf:
                        yaml.dump(configs, cf)
                    status = f"New RDF graph: {filename}. Query history deleted. "
            elif form.submit_add.data:
                if not form.file_field_rdf.data:
                    status = "Select file first!"
                else:
                    filename = form.file_field_rdf.data.filename
                    parse_rdf_file(g, form.file_field_rdf.data)
                    configs['import_list'].append(filename)
                    with open(CONFIGS, 'w') as cf:
                        yaml.dump(configs, cf)
                    status = f"Added RDF graph: {filename}"
            elif form.submit_save.data:
                g.serialize(destination=REPO)
                status = f"Saved graph to repo!"
            elif form.submit_download.data:
                g.serialize(destination=REPO)
                logging.info(f"Downloaded graph")
                graph_io = io.BytesIO()
                with zipfile.ZipFile(graph_io, mode='w') as z:
                    z.write(REPO)
                graph_io.seek(0)
                return send_file(graph_io, as_attachment=True, download_name='repo.zip', mimetype='application/zip')
            elif form.submit_csn_json.data:
                name = os.path.splitext(configs['import_list'])[0]
                csn_json = ttl2csn.ttl2json(g, QUERY_CSN_JSON_FILE, name)
                filename = os.path.join('data', name) + '_ER_Model.json'
                with open(filename, mode='w') as js:
                    js.write(csn_json)
                logging.info(f"Download converted ER-model (json): {filename}")
                return send_file(filename, as_attachment=True, download_name=os.path.basename(filename))
            else:
                submit_filter = form.submit_new.data or form.submit_add.data or form.submit_save.data or \
                                form.submit_download.data or form.submit_csn_json.data
                logging.error(f"The filter and the selection does not match? {submit_filter}")
                raise ValueError(f"The filter and the selection does not match? {submit_filter}")

            return render_template('import_rdf.html', form=form,
                                   rdflist_header=['Imports'], rdflist_body=configs['import_list'],
                                   result_header=[], result_body=[],
                                   status=status)

    # Backward query history
    if form.submit_back.data:
        logging.info(f"Backward query history")
        form.textarea_cmd.data = history_query.back()
        return render_template('import_rdf.html', form=form,
                               rdflist_header=['Imports'], rdflist_body=configs['import_list'],
                               result_header=[], result_body=[],
                               status=f'Back in query history: {history_query.pointer_str()}')
    # forward query history
    elif form.submit_forward.data:
        logging.info(f"Forward query history")
        form.textarea_cmd.data = history_query.forward()
        return render_template('import_rdf.html', form=form,
                               rdflist_header=['Imports'], rdflist_body=configs['import_list'],
                               result_header=[], result_body=[],
                               status=f'Forward in query history: {history_query.pointer_str()}')
    # Copy Query
    elif form.submit_use_query.data:
        logging.info(f"Copy query to SPARQL-field")
        form.textarea_cmd.data = stored_queries[form.selected_query.data]
        return render_template('import_rdf.html', form=form,
                               rdflist_header=['Imports'], rdflist_body=configs['import_list'],
                               result_header=[], result_body=[],
                               status='Copied query')
    # Run Query
    elif form.submit_run.data:
        logging.info(f'Query: {form.textarea_cmd.data}')
        history_query.append(str(form.textarea_cmd.data))
        try:
            render_template('import_rdf.html', form=form,
                            rdflist_header=['Imports'], rdflist_body=configs['import_list'],
                            result_header=[], result_body=[],
                            status='Query running...')
            start_time = datetime.now()
            statement = form.textarea_cmd.data
            logging.info(f"Query: {statement}")
            if re.match(r'\s*INSERT\s+.+', statement):
                query_results = g.update(statement)
                query_statement = False
            elif re.match(r'\s*SELECT\s+.+', statement):
                query_results = g.query(statement)
                query_statement = True
            else:
                logging.error(f'Unknown query? {statement}')
                raise pyparsing.exceptions.ParseException(f'Unknown query (not implemented)?')
            run_time = (datetime.now() - start_time).total_seconds()
        except Exception as pe:
            logging.error(pe)
            return render_template('import_rdf.html', form=form,
                                   rdflist_header=['Imports'], rdflist_body=configs['import_list'],
                                   result_header=[], result_body=[],
                                   status=f"Parsing error: {pe}")
        else:
            if query_statement:
                if len(query_results.vars) > 1:
                    if form.check_use_namespaces.data:
                        if form.check_unquote.data:
                            result_body = [
                                [urllib.parse.unquote(r[v].n3(g.namespace_manager)) for v in query_results.vars
                                 if r[v]] for r in query_results]
                        else:
                            result_body = [
                                [r[v].n3(g.namespace_manager) for v in query_results.vars
                                 if r[v]] for r in query_results]

                    else:
                        if form.check_unquote.data:
                            result_body = [[urllib.parse.unquote(r[v]) for v in query_results.vars
                                            if r[v]] for r in query_results]
                        else:
                            result_body = [[r[v] for v in query_results.vars
                                            if r[v]] for r in query_results]
                else:
                    var = query_results.vars[0]
                    if form.check_use_namespaces.data:
                        if form.check_unquote.data:
                            result_body = [[urllib.parse.unquote(i)] for i in set([r[var].n3(g.namespace_manager) for r in query_results])]
                        else:
                            result_body = [[i] for i in
                                           set([r[var].n3(g.namespace_manager) for r in query_results])]
                    else:
                        if form.check_unquote.data:
                            result_body = [urllib.parse.unquote(i) for i in set([r[var] for r in query_results])]
                        else:
                            result_body = [i for i in set([r[var] for r in query_results])]

                return render_template('import_rdf.html', form=form,
                                       rdflist_header=['Imports'], rdflist_body=configs['import_list'],
                                       result_header=query_results.vars, result_body=result_body,
                                       status=f'Query runtime: {run_time}')
            else:
                return render_template('import_rdf.html', form=form,
                                       rdflist_header=['Imports'], rdflist_body=configs['import_list'],
                                       result_header=[], result_body=[],
                                       status=f'Insert runtime: {run_time}')

    # Save Query
    elif form.submit_save_query.data:
        logging.info(f'Query saved: {form.textarea_cmd.data}')
        stored_queries[form.save_text.data] = form.textarea_cmd.data
        with open(QUERY_FILE, 'w') as qr:
            json.dump(stored_queries, qr, indent=4)

    elif form.submit_reasoning.data:
        logging.info(f'Start Deductive Closure ("RDFS_Semantics","OWLRL_Semantics")')
        DeductiveClosure(RDFS_Semantics).expand(g)
        DeductiveClosure(OWLRL_Semantics).expand(g)

    return render_template('import_rdf.html', form=form,
                           rdflist_header=['Imports'], rdflist_body=configs['import_list'],
                           result_header=[], result_body=[],
                           status=status)


if __name__ == '__main__':
    app.run('0.0.0.0', port=5000)
