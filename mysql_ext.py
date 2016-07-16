import configparser
import os

import mysql.connector


class db(object):

    connections = dict()
    level = dict()

    @staticmethod
    def configure(configuration_file=None):
        if configuration_file is None:
            configuration_file = os.path.join(os.path.expanduser('~'), '.mysql_dbs.cfg')
        conf = configparser.ConfigParser()
        conf.read(configuration_file)
        db.known_databases = dict()
        for section in conf.sections():
            database_options = dict()
            for option in conf.options(section):
                database_options[option] = conf.get(section, option).strip()
            db.known_databases[section] = database_options

    def __init__(self, name, **kwargs):
        self.name = name
        if name not in db.connections:
            for key, value in db.known_databases.get(name, dict()).items():
                if key not in kwargs:
                    kwargs[key] = value
            if 'database' not in kwargs:
                kwargs['database'] = name
            if 'host' not in kwargs:
                kwargs['host'] = '127.0.0.1'
            db.connections[name] = mysql.connector.connect(**kwargs)
        self.cnx = db.connections[name]
        self.queries = []

    def __enter__(self):
        if not self.cnx.is_connected():
            self.cnx.reconnect()
        if self.name not in db.level:
            db.level[self.name] = 1
        else:
            db.level[self.name] += 1
        self.cursor = self.cnx.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        db.level[self.name] -= 1
        if db.level[self.name] == 0:
            if exc_type is None:
                self.cnx.commit()
            else:
                self.cnx.rollback()
        self.cursor.close()

    def __call__(self, query, *args):
        try:
            self.cursor.execute(query, *args)
        except:
            print('_' * (len(self.cursor.statement or '') + 3))
            print(self.cursor.statement)
            print("")
            raise

    def select(self, table, *expressions, **where):
        selects = ', '.join(expressions)
        dictionary = dict()
        if where:
            cmd = ['SELECT', selects or 'id', 'FROM', table, 'WHERE', (self._where(dictionary, where))]
        else:
            cmd = ['SELECT', selects or 'id', 'FROM', table]
        self(' '.join(cmd), dictionary)
        return [row[0] if len(row) == 1 else row for row in self.cursor]

    def insert(self, table, **assignments):
        cmd = ['INSERT', table, 'SET', (self._set(assignments))]
        self(' '.join(cmd), assignments)
        return self.cursor.lastrowid

    def update(self, table, where, **assignments):
        cmd = ['UPDATE', table, 'SET', (self._set(assignments)), 'WHERE', db._where_item(assignments, 'id', where)]
        self(' '.join(cmd), assignments)
        return self.cursor.rowcount

    def delete(self, table, **where):
        dictionary = dict()
        cmd = ['DELETE', 'FROM', table, 'WHERE', db._where_item(dictionary, 'id', where)]
        self(' '.join(cmd), dictionary)
        return self.cursor.rowcount

    @staticmethod
    def _set(assignments):
        return ','.join(['%s = %%(%s)s' % (key, key) for key in assignments])

    @staticmethod
    def _where(dictionary, predicates):
        return ' AND '.join([db._where_item(dictionary, key, val) for key, val in predicates.items()])

    @staticmethod
    def _where_item(dictionary, key, val):
        if val is None:
            return '%s IS NULL' % key
        elif isinstance(val, int):
            return '%s = %d' % (key, val)
        elif isinstance(val, (tuple, list, set, frozenset)):
            return '(%s)' % ' OR '.join([db._where_item(dictionary, key, value) for value in val])
        elif isinstance(val, dict):
            return '(%s)' % db._where(dictionary, val)
        else:
            sub_key = '%s#%s' % (key, len(dictionary))
            dictionary[sub_key] = val
            return '%s = %%(%s)s' % (key, sub_key)
