import configparser
import datetime
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
        self._execute_one(query, *args)
        return list(self.cursor)

    def create(self, table, **columns):
        definitions = ['id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY']
        for name, definition in columns.items():
            if isinstance(definition, tuple):
                if (len(definition)) == 1:
                    definitions.append(' '.join([name, db._make_definition(definition[0], False, None)]))
                elif len(definition) == 2 and definition[0] is None:
                    definitions.append(' '.join([name, db._make_definition(definition[1], True, None)]))
                elif len(definition) == 2 and definition[1] is None:
                    definitions.append(' '.join([name, db._make_definition(definition[0], True, None)]))
                elif len(definition) == 2:
                    definitions.append(' '.join([name, db._make_definition(definition[0], False, definition[1])]))
                elif len(definition) == 3 and definition[0] is None:
                    definitions.append(' '.join([name, db._make_definition(definition[1], True, definition[2])]))
                elif len(definition) == 3 and definition[1] is None:
                    definitions.append(' '.join([name, db._make_definition(definition[0], True, definition[2])]))
            elif isinstance(definition, str):
                definitions.append(definition)
            else:
                definitions.append(' '.join([name, db._make_definition(definition, False, None)]))
        cmd = ['CREATE', 'TABLE', table, '(%s)' % ', '.join(definitions)]
        self(' '.join(cmd))

    def select(self, table, *expressions, **where):
        selects = ', '.join(expressions)
        dictionary = dict()
        if where:
            cmd = ['SELECT', selects or 'id', 'FROM', table, 'WHERE', (self._where(dictionary, where))]
        else:
            cmd = ['SELECT', selects or 'id', 'FROM', table]
        self._execute_one(' '.join(cmd), dictionary)
        return [row[0] if len(row) == 1 else row for row in self.cursor]

    def insert(self, table, *args, **assignments):
        if args:
            columns = set()
            for row in args:
                columns |= set(row.keys())
            columns = list(columns)
            cmd = ['INSERT', table, 'SET', ', '.join(['%s = %%s' % column for column in columns])]
            rows = [tuple(row.get(column, assignments.get(column)) for column in columns) for row in args]
            self._execute_many(' '.join(cmd), rows)
        elif any([isinstance(value, list) for value in assignments.values()]):
            assignments = list(assignments.items())
            cmd = ['INSERT', table, 'SET', ', '.join(['%s = %%s' % column for column, _ in assignments])]
            rows = [tuple([value[index] if isinstance(value, list) else value for _, value in assignments])
                    for index in range(min([len(value) for _, value in assignments if isinstance(value, list)]))]
            self._execute_many(' '.join(cmd), rows)
        else:
            cmd = ['INSERT', table, 'SET', (self._set(assignments))]
            self._execute_one(' '.join(cmd), assignments)
        return self.cursor.lastrowid

    def update(self, table, where, **assignments):
        cmd = ['UPDATE', table, 'SET', (self._set(assignments)), 'WHERE', db._where_item(assignments, 'id', where)]
        self._execute_one(' '.join(cmd), assignments)
        return self.cursor.rowcount

    def delete(self, table, **where):
        dictionary = dict()
        cmd = ['DELETE', 'FROM', table, 'WHERE', db._where_item(dictionary, 'id', where)]
        self._execute_one(' '.join(cmd), dictionary)
        return self.cursor.rowcount

    @staticmethod
    def _make_definition(definition_type, nullable, size):
        if definition_type == id:
            definition = 'INT UNSIGNED'
        elif definition_type == bool:
            definition = 'TINYINT'
        elif definition_type == int:
            if size is None:
                definition = 'INT'
            elif size < 3:
                definition = 'TINYINT'
            elif size < 5:
                definition = 'SMALLINT'
            elif size < 7:
                definition = 'MEDIUMINT'
            elif size < 10:
                definition = 'INT'
            else:
                definition = 'BIGINT'
        elif definition_type == float:
            definition = 'REAL'
        elif definition_type == str:
            if size is None:
                definition = 'TEXT'
            elif size < 2**8:
                definition = 'VARCHAR(%d)' % size
            elif size < 2**16:
                definition = 'TEXT' % size
            elif size < 2**24:
                definition = 'MEDIUMTEXT' % size
            else:
                definition = 'LONGTEXT'
        elif definition_type == datetime.datetime:
            if size is None:
                definition = 'DATETIME(6)'
            else:
                definition = 'DATETIME(%d)' % size
        elif definition_type == datetime.time:
            if size is None:
                definition = 'TIME(6)'
            else:
                definition = 'TIME(%d)' % size
        elif definition_type == datetime.date:
            definition = 'DATE'
        if not nullable:
            definition = ' '.join([definition, 'NOT NULL'])
        return definition

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

    def _execute_one(self, query, *args):
        try:
            self.cursor.execute(query, *args)
        except:
            print('_' * (len(self.cursor.statement or '') + 3))
            print(self.cursor.statement)
            print("")
            raise

    def _execute_many(self, query, *args):
        try:
            self.cursor.executemany(query, *args)
        except:
            print('_' * (len(self.cursor.statement or '') + 3))
            print(self.cursor.statement)
            print("")
            raise




