#
#  SPDX-FileCopyrightText: 2022 Thorsten Hapke <thorsten.hapke@sap.com>
#
#  SPDX-License-Identifier: Apache-2.0
#
import json
import argparse
import csv
import logging
import os
import re

from rdflib import Graph, URIRef, Literal, Namespace
from rdflib.namespace import RDF, RDFS, XSD

from tabulate import tabulate


# Global variables
class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    END = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


map_cds_datatypes = {
    "TIME": "cds.LocalTime",
    "DATE": "cds.LocalDate",
    "DECIMAL": "cds.Decimal",
    "BINARY": "cds.Binary",
    "INTEGER": "cds.Integer",
    "STRING": "cds.String",
    "DATETIME": "cds.UTCTimestamp",
    "BOOLEAN": "cds.Boolean"
}

dimd = Namespace("https://www.sap.com/products/data-intelligence#")
QUERY_FILE = 'rdf_resources/queries.csv'


def read_query_csv(filename):
    queries = dict()
    with open(filename,'r') as fp:
        reader = csv.reader(fp)
        for line in reader:
            if line[0][0] == '#':
                continue
            queries[line[0]] = {"query": line[1]}
            if len(line) > 2:
                queries[line[0]]['variables'] = line[4:]
    logging.info(f"Read queries SELECT: {len(queries)}")
    return queries


def build_query(query_dict, variables=None):
    """
    Build query with variables
    :param query_dict: dict of query
    :param variables: dict of replacements
    :return: query string
    """
    query_str = query_dict['query']
    if not variables:
        return query_dict['query']

    for k, v in variables.items():
        query_str = query_str.replace(k, v)

    return query_str


def query(graph, statement):
    """
    Sends query to graph and converts the result into list of dict. Result string produced as well
    :param graph: graph
    :param statement: query statement
    :return: list of dict, str
    """
    query_result = graph.query(statement)
    if len(query_result.vars) > 1:
        results = [{str(v): r[v] for v in query_result.vars} for r in query_result]
        str_results = tabulate([[v for v in r.values()]for r in results], headers=query_result.vars)
    else:
        var = query_result.vars[0]
        results = set([r[var] for r in query_result])
        str_results = tabulate([[r] for r in results], headers=query_result.vars)
    return results, str_results


def ttl2json(g, query_file, name, table_ref=None):

    queries = read_query_csv(query_file)
    # 1. Get all tables
    query_statement = queries['GET_TABLES']
    logging.debug(f"Tables query: {query_statement}")
    tables_list, print_str = query(g, query_statement['query'])
    # print(print_str)

    tables = dict()
    for table in tables_list:
        table_label = str(table['label'])
        tables[table_label] = {"kind": 'entity', "@EndUserText.label": str(table['comment']), "elements": dict()}
        query_statement = build_query(queries['GET_TABLE_COLUMNS'], {"TABLE": str(table['url'])})
        # logging.debug(f"Columns query: {query_statement}")
        columns, print_str = query(g, query_statement)
        # print(print_str)
        for col in columns:
            col_label = str(col['label'])
            tables[table_label]['elements'][col_label] = {"@EndUserText.label": str(col['comment'])}
            query_statement = build_query(queries['GET_COLUMN_ATTRIBUTES'], {"COLUMN": str(col['url'])})
            # logging.debug(f"Attributes query: {query_statement}")
            attributes, print_str = query(g, query_statement)
            # print(print_str)
            for att in attributes:
                table_attribute = tables[table_label]['elements'][col_label]
                match att['pred']:
                    case dimd.length:
                        try:
                            table_attribute['length'] = int(att['obj'])
                        except ValueError as ve:
                            if att['obj']:
                                table_attribute['length'] = 1
                    case dimd.datatype:
                        dt = str(att['obj'])
                        if dt not in map_cds_datatypes:
                            raise ValueError(f"Datatype \'{dt}\' not in map_cds_datatypes!")
                        table_attribute['type'] = map_cds_datatypes[dt]
                    case dimd.precision:
                        table_attribute['precision'] = int(att['obj'])
                    case dimd.scale:
                        table_attribute['scale'] = int(att['obj'])
                    case dimd.foreignReference:
                        target_table = re.match(r".*\/(\w+)\/\w+$", str(att['obj'])).group(1)
                        target_column = re.match(r".*\/(\w+)$", str(att['obj'])).group(1)
                        target_ref = '_' + target_table
                        table_attribute['@ObjectModel.foreignKey.association'] = {'=': target_ref}
                        if target_ref not in tables[table_label]['elements']:
                            tables[table_label]['elements'][target_ref] = {
                                "@EndUserText.label": f"{table_label} to {target_table}",
                                "target": target_table,
                                "type": "cds.Association",
                                "on": list()}
                        if len(tables[table_label]['elements'][target_ref]['on']) > 0:
                            tables[table_label]['elements'][target_ref]['on'].append('and')
                        tables[table_label]['elements'][target_ref]['on'].extend([
                            {"ref": [col_label]}, '=', {"ref": [target_ref, target_column]}])

    # Add metadata
    csn_dict = {
        "version": {"csn": "1.0"},
        "$version": "1.0",
        "meta": {
            "creator": "ttl2csn",
            "kind": "sap.dwc.ermodel",
            "label": os.path.splitext(os.path.basename(name))[0]
          },
        "definitions": tables
    }

    return json.dumps(csn_dict, indent=4)


if __name__ == '__main__':

    # read graph repository
    g = Graph()
    g.parse(os.path.join('../data', 'repo.ttl'))
    dimd = Namespace("https://www.sap.com/products/data-intelligence#")
    g.bind("dimd", dimd)

    csnjson = ttl2json(g, os.path.join('../data', 'ttl2csn_queries.csv'), "di modelled")
    print(csnjson)
