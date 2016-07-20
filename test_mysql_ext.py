import datetime
from unittest import TestCase

import mysql.connector

import mysql_ext


class TestDb(TestCase):

    @staticmethod
    def setUpClass():
        mysql_ext.db.add_database('test', user='test', password='test1234')

    def setUp(self):
        self.date1 = datetime.datetime(2015, 1, 2, 10, 20, 30, 123456)
        self.text1 = 'This is a text.'
        self.date2 = datetime.datetime(2016, 1, 2, 10, 20, 30, 123456)
        self.text2 = 'This is another text.'
        with mysql_ext.db('test') as query:
            query.create('texts', date=datetime.datetime, text=(str, None, 30))

    def basic_insert(self, query: mysql_ext.db):
        query.insert('texts', date=self.date1, text=self.text1)
        query.insert('texts', date=self.date2, text=self.text2)

    def tearDown(self):
        with mysql_ext.db('test') as query:
            query('DROP TABLE texts')
            query('DROP TABLE IF EXISTS test')

    def test_create(self):
        with mysql_ext.db('test') as query:
            query.create('test', time=datetime.time, text_id=id)
            self.assertListEqual([('id', 'int(10) unsigned', 'NO', 'PRI', None, 'auto_increment'),
                                  ('text_id', 'int(10) unsigned', 'NO', '', None, ''),
                                  ('time', 'time(6)', 'NO', '', None, '')],
                                 sorted(query('SHOW COLUMNS FROM test')))

    def test_select(self):
        with mysql_ext.db('test') as query:
            self.basic_insert(query)
            self.assertListEqual([self.text1, self.text2], query.select('texts', 'text'))
            self.assertListEqual([self.text1], query.select('texts', 'text', date=self.date1))
            self.assertListEqual([self.text2], query.select('texts', 'text', date=self.date2))

    def test_select_named_tuple(self):
        with mysql_ext.db('test') as query:
            self.basic_insert(query)
            rows = query.select('texts', '*')
            self.assertListEqual([1, 2], [row.id for row in rows])
            self.assertListEqual([self.date1, self.date2], [row.date for row in rows])
            self.assertListEqual([self.text1, self.text2], [row.text for row in rows])

    def test_no_selects(self):
        with mysql_ext.db('test') as query:
            self.basic_insert(query)
            self.assertListEqual([], query.select('texts', text=[]))

    def test_select_ids(self):
        with mysql_ext.db('test') as query:
            self.basic_insert(query)
            self.assertListEqual([1, 2], query.select('texts'))
            self.assertListEqual([1], query.select('texts', date=self.date1))
            self.assertListEqual([2], query.select('texts', date=self.date2))

    def test_select_by_ids(self):
        with mysql_ext.db('test') as query:
            self.basic_insert(query)
            self.assertListEqual([self.text1, self.text2], query.select('texts', 'text', id=[1, 2]))
            self.assertListEqual([self.text1], query.select('texts', 'text', id=1))
            self.assertListEqual([self.text2], query.select('texts', 'text', id=2))

    def test_insert_many(self):
        with mysql_ext.db('test') as query:
            self.assertListEqual([1, 2, 3], list(query.insert('texts',
                                                              dict(date=self.date1, text=self.text1),
                                                              dict(date=self.date2, text=self.text2),
                                                              dict(date=self.date1))))
            self.assertListEqual([self.text1, self.text2, None], query.select('texts', 'text'))

    def test_insert_many_with_defaults(self):
        with mysql_ext.db('test') as query:
            self.assertEqual(range(1, 4), query.insert('texts',
                                                       dict(date=self.date1, text=self.text1),
                                                       dict(date=self.date2, text=self.text2),
                                                       dict(date=self.date1),
                                                       text=self.text2))
            self.assertListEqual([self.text1, self.text2, self.text2], query.select('texts', 'text'))

    def test_insert_many_as_lists(self):
        with mysql_ext.db('test') as query:
            self.assertListEqual([1, 2], list(query.insert('texts',
                                                           date=[self.date1, self.date2, self.date1],
                                                           text=[self.text1, self.text2])))
            self.assertListEqual([self.text1, self.text2], query.select('texts', 'text'))

    def test_insert_many_as_lists_with_default(self):
        with mysql_ext.db('test') as query:
            self.assertEqual(range(1, 4), query.insert('texts',
                                                       date=[self.date1, self.date2, self.date1],
                                                       text=self.text1))
            self.assertListEqual([self.text1, self.text1, self.text1], query.select('texts', 'text'))

    def test_update(self):
        with mysql_ext.db('test') as query:
            self.basic_insert(query)
            text = "hello world"
            self.assertEqual(1, query.update('texts', 2, text=text))
            self.assertEqual(0, query.update('texts', dict(text=text), text=text))
            self.assertListEqual([self.text1, text], query.select('texts', 'text'))

    def test_no_updates(self):
        with mysql_ext.db('test') as query:
            self.basic_insert(query)
            self.assertEqual(0, query.update('texts', [], text="dummy"))
            self.assertListEqual([self.text1, self.text2], query.select('texts', 'text'))

    def test_delete(self):
        with mysql_ext.db('test') as query:
            self.basic_insert(query)
            self.assertEqual(1, query.delete('texts', text=self.text1))
            self.assertListEqual([2], query.select('texts'))

    def test_no_deletes(self):
        with mysql_ext.db('test') as query:
            self.basic_insert(query)
            self.assertEqual(0, query.delete('texts', id=[]))
            self.assertListEqual([1, 2], query.select('texts'))
