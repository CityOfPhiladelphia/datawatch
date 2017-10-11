from urllib.parse import urlparse
import re
import os

import requests

carto_connection_string_regex = r'^carto://'

connections = {}

def get_connection(config, name):
    if name in connections:
        return connections[name]

    if name not in config['connections']:
        raise Exception('Connection `{}` not found in config'.format(name))

    connection_string = config['connections'][name]

    if connection_string[0] == '$':
        connection_string = os.getenv(connection_string[1:])

    if urlparse(connection_string).scheme == 'carto':
        connection = CartoConnection(connection_string)
    else:
        connection = SQLConnection(connection_string)
    
    connections[name] = connection

    return connection

def close_connections():
    for connection in connections.values():
        connection.close()

class Connection(object):
    def query(self, query_str):
        raise NotImplementedError()

    def count(self, table_name):
        raise NotImplementedError()

    def close(self):
        raise NotImplementedError()

class CartoConnection(Connection):
    def __init__(self, connection_string):
        url_parts = urlparse(connection_string)
        self.user = url_parts.username
        self.api_key = url_parts.password
        self.host = url_parts.hostname
        self.path = url_parts.path

        self.session = requests.Session()

    def query(self, query_str):
        if self.host == None and self.username != None:
            url = 'https://{}.carto.com/api/v2/sql'.format(self.username)
        else:
            if self.path:
                url = 'https://{}{}'.format(self.host, self.path)
            else:
                url = 'https://{}/api/v2/sql'.format(self.host)

        qs = {}
        if self.api_key:
            qs['api_key'] = self.api_key

        response = self.session.post(
            url,
            qs=qs,
            data={
                'q': query_str
            })
        response.raise_for_status()
        return response.json()['rows'], response.status_code

    def count(self, table_name):
        return self.query('select count(*) from "{}"'.format(table_name))[0]['count']

    def close(self):
        self.session.close()

class SQLConnection(Connection):
    def __init__(self, connection_string):
        self.engine = create_engine(connection_string)

    def query(self, query_str):
        if not self.connection:
            self.connection = self.engine.connect()

        result = self.connection.execute(query_str)
        return result.fetchall(), None

    def count(self, table_name):
        ## TODO: surround table_name with dialect specific encapsulation - ex ` "
        return self.query('select count(*) as count from {}'.format(table_name))[0]['count']

    def close(self):
        if self.connection:
            self.connection.close()
