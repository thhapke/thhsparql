import os
from urllib.parse import urljoin
import urllib
import json
import yaml

import requests
import logging


#  GET Datasets
#
def get_datasets(connection, connection_id, dataset_path):
    qualified_name = urllib.parse.quote(dataset_path, safe='')  # quote to use as  URL component
    restapi = f"/catalog/connections/{connection_id}/containers/{qualified_name}"
    url = connection['url'] + restapi
    headers = {'X-Requested-With': 'XMLHttpRequest'}
    logging.info(f"Request URL: {url}")
    r = requests.get(url, headers=headers, auth=connection['auth'])

    if r.status_code != 200:
        logging.error(f"Status code: {r.status_code}  - {r.text}")
        return None

    return json.loads(r.text)['datasets']


#
#  GET Tags of datasets
#
def get_dataset_factsheets(connection, connection_id, dataset_path):
    qualified_name = urllib.parse.quote(dataset_path, safe='')  # quote to use as  URL component
    restapi = f"/catalog/connections/{connection_id}/datasets/{qualified_name}/factsheet"
    url = connection['url'] + restapi
    headers = {'X-Requested-With': 'XMLHttpRequest'}
    params = {"connectionId": connection_id, "qualifiedName": dataset_path}
    r = requests.get(url, headers=headers, auth=connection['auth'], params=params)

    if r.status_code != 200:
        logging.error(f"Status code: {r.status_code}  - {r.text}")
        return None
    return json.loads(r.text)


#
#  GET Tags of datasets
#
def get_dataset_tags(connection, connection_id, dataset_path):
    qualified_name = urllib.parse.quote(dataset_path, safe='')  # quote to use as  URL component
    restapi = f"/catalog/connections/{connection_id}/datasets/{qualified_name}/tags"
    url = connection['url'] + restapi
    headers = {'X-Requested-With': 'XMLHttpRequest'}
    params = {"connectionId": connection_id, "qualifiedName": dataset_path}
    r = requests.get(url, headers=headers, auth=connection['auth'], params=params)

    if r.status_code != 200:
        logging.error(f"Status code: {r.status_code}  - {r.text}")
        return None
    return json.loads(r.text)


#
#  GET Lineage of datasets
#
def get_dataset_lineage(connection, connection_id, dataset_path):
    restapi = f"/catalog/lineage/export"
    url = connection['url'] + restapi
    headers = {'X-Requested-With': 'XMLHttpRequest'}
    params = {"connectionId": connection_id, "qualifiedNameFilter": dataset_path}
    r = requests.get(url, headers=headers, auth=connection['auth'], params=params)

    if r.status_code == 404:
        logging.warning(f"Status code: {r.status_code}  - No lineage found for: {dataset_path}")
        return None
    if r.status_code == 500:
        logging.error(f"Status code: {r.status_code}  - {r.text}")
        return None
    return json.loads(r.text)


def export_catalog(host, tenant, path, user, password, connection_id, container):
    """
    Exports catalog datasets
    :param host: di system url
    :param tenant:
    :param user:
    :param password:
    :param connection_id:
    :param container: 
    :return: exported data as dict
    """
    connection = {'url': urljoin(host, path), 'auth': (tenant + '\\' + user, password)}
    tags = True
    lineage = True

    # Get all catalog datasets under container path
    logging.info(f"Get datasets: {connection_id} - {container}")
    datasets = get_datasets(connection, connection_id, container)

    if not datasets:
        logging.info(f"No Datasets for: {connection_id} - {container} -> shutdown pipeline")
        return None

    dataset_factsheets = list()
    for i, ds in enumerate(datasets):

        # skip erroneous datasets
        if ds['remoteObjectReference']['remoteObjectType'] == 'FILE.UNKNOWN' or \
                ('size' in ds['remoteObjectReference'] and ds['remoteObjectReference']['size'] == 0):
            continue

        qualified_name = ds['remoteObjectReference']['qualifiedName']
        logging.info(f'Get dataset metadata: {qualified_name}')
        dataset = get_dataset_factsheets(connection, connection_id, qualified_name)

        # In case of Error (like imported data)
        if not dataset:
            continue

        if tags:
            dataset['tags'] = get_dataset_tags(connection, connection_id, qualified_name)

        if lineage:
            lineage_info = get_dataset_lineage(connection, connection_id, qualified_name)
            if lineage_info:
                dataset['lineage'] = lineage_info

        dataset_factsheets.append(dataset)

    return dataset_factsheets


if __name__ == '__main__':
    with open(os.path.join('../data/config_collibra.yaml')) as fp:
        config = yaml.safe_load(fp)
    host = config['host']
    tenant = config['tenant']
    path = '/app/datahub-app-metadata/api/v1'
    user = config['user']
    password = config['password']
    exported_data = export_catalog(host, tenant, path, user, password, connection_id='S4HANA', container='/TABLES')

    print(json.dumps(exported_data, indent=4))