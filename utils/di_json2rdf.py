import logging
import os.path
import urllib.parse

from rdflib import Graph, URIRef, Literal, Namespace
from rdflib.namespace import RDF, RDFS, XSD
from owlrl import RDFS_Semantics, DeductiveClosure

map_xsd_datatypes = {
    "TIME": "xsd:time",
    "DATE": "xsd:date",
    "DECIMAL": "xsd:decimal",
    "BINARY": "xsd:base64Binary",
    "INTEGER": "xsd:integer",
    "STRING": "xsd:string",
    "DATETIME": "xsd.dateTime",
    "BOOLEAN": "xsd.boolean",
    "FLOAT": "xsd:float",
    "DOUBLE": "xsd:double"
}

suffices = ['.csv', '.json', '.txt', '.xml', '.yaml', '.log', '.cfg', '.parquet', '.orc']


#
# DATA INPUT
#
def to_rdf(data, instance, deductive_closure=True):
    # create Graph
    g = Graph()
    g.parse('dimd.ttl')
    dimd = Namespace("https://www.sap.com/products/data-intelligence#")
    g.bind("dimd", dimd)
    g.bind("instance", instance)

    for dataset in data:
        dataset_path = urllib.parse.unquote(dataset['metadata']['uri'])
        if '.' in dataset_path:
            dataset_path, suffix = os.path.splitext(dataset_path)
        if dataset_path[0] == '/':
            dataset_path = dataset_path[1:]
        if dataset_path[-1] == '/':
            dataset_path = dataset_path[:-1]
        dataset_uri = instance[urllib.parse.quote(dataset_path, safe='')]
        dataset_type = dataset['metadata']['type'].capitalize()
        g.add((dataset_uri, RDF.type, dimd[dataset_type]))
        g.add((dataset_uri, RDFS.label, Literal(dataset['metadata']['name'])))

        # comment = descriptions
        if "descriptions" in dataset['metadata']:
            for d in dataset['metadata']['descriptions']:
                if d['type'] == 'SHORT':
                    g.add((dataset_uri, RDFS.comment, Literal(d['value'])))
        else:
            g.add((dataset_uri, RDFS.comment, Literal('No Description')))

        # primary keys
        # unique_keys = list()
        if "uniqueKeys" in dataset["metadata"]:
            for uk in dataset["metadata"]["uniqueKeys"]:
                for pk in uk["attributeReferences"]:
                    column_path = urllib.parse.quote(dataset_path + '/' + column['name'], safe='')
                    column_uri = instance[column_path]
                    g.add((dataset_uri, dimd.primaryKey, column_uri))
                    g.add((column_uri, dimd.key, Literal(True)))

        # COLUMNS
        for column in dataset['columns']:
            column_path = urllib.parse.quote(dataset_path + '/' + column['name'], safe='')
            column_uri = instance[column_path]
            g.add((column_uri, RDFS.label, Literal(column['name'])))
            g.add((dataset_uri, dimd.column, column_uri))
            g.add((column_uri, RDF.type, dimd.Column))
            g.add((column_uri, dimd.datatype, Literal(column['type'])))
            if column['type'] in map_xsd_datatypes:
                g.add((column_uri, RDFS.range, Literal(map_xsd_datatypes[column['type']])))
            else:
                logging.warning(f"No XSD mapping for {column['type']}")
            g.add((column_uri, dimd.templateDataType, Literal(column['templateType'])))
            if 'length' in column:
                g.add((column_uri, dimd.length, Literal(column['length'])))
            if 'precision' in column:
                g.add((column_uri, dimd.precision, Literal(column['precision'])))
            if 'scale' in column:
                g.add((column_uri, dimd.scale, Literal(column['scale'])))
            if "descriptions" in column:
                for d in column['descriptions']:
                    if d['type'] == 'SHORT':
                        g.add((column_uri, RDFS.comment, Literal(d['value'])))
                        break

        # TAGS
        if 'tags' in dataset:
            if 'tagsOnDataset' in dataset['tags']:
                for dtag in dataset['tags']['tagsOnDataset']:
                    hierarchy_path = f"/hierarchy/{dtag['hierarchyName']}"
                    for tag in dtag['tags']:
                        tag_path = hierarchy_path + '/' + tag['tag']['path']
                        tag_uri = instance[urllib.parse.quote(tag_path, safe='')]
                        g.add((dataset_uri, dimd.tag, tag_uri))
            if 'tagsOnAttribute' in dataset['tags']:
                for atag in dataset['tags']['tagsOnAttribute']:
                    column_uri = instance[urllib.parse.quote(dataset_path + '/' + atag['attributeQualifiedName'],
                                                             safe='')]
                    for tag in atag['tags']:
                        hierarchy_path = '/hierarchy/' + tag['hierarchyName']
                        for tag2 in tag['tags']:
                            tag_path = hierarchy_path + '/' + tag2['tag']['path'].replace('.', '/')
                            tag_uri = instance[urllib.parse.quote(tag_path, safe='')]
                            g.add((column_uri, dimd.tag, tag_uri))
                            if tag['hierarchyName'] == 'AlternativeLabels':
                                g.add((column_uri, RDFS.label, Literal(tag2['tag']['name'])))

        # LINEAGE
        if 'lineage' in dataset and isinstance(dataset['lineage'], dict):
            logging.info(f"Lineage of dataset: {dataset['metadata']['uri']}")
            for pcn in dataset['lineage']['publicComputationNodes']:
                for transform in pcn['transforms']:
                    for computation in transform['datasetComputation']:
                        if 'inputDatasets' not in computation or 'outputDatasets' not in computation:
                            logging.info(
                                f"No inputDatasets or outputDatasets in lineage for {dataset['metadata']['uri']}")
                            continue
                        else:
                            in_uris = [instance[urllib.parse.quote(ind['externalDatasetRef'], safe='')]
                                       for ind in computation['inputDatasets']]
                            out_uris = [instance[urllib.parse.quote(ind['externalDatasetRef'], safe='')]
                                        for ind in computation['outputDatasets']]
                        for i in in_uris:
                            for o in out_uris:
                                g.add((i, dimd.lineage, o))
                                g.add((i, dimd.computationType, Literal(computation['computationType'])))
                                g.add((o, dimd.impact, i))

    # Expand graph for RDFS semantics
    if deductive_closure:
        DeductiveClosure(RDFS_Semantics).expand(g)

    return g
