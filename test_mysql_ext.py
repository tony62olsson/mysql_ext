import datetime
from unittest import TestCase

import mysql_ext

mysql_ext.db.configure()


class TestDb(TestCase):

    def setUp(self):
        self.date1 = datetime.datetime(2015, 1, 2, 10, 20, 30, 123456)
        self.text1 = 'This is a text.'
        self.date2 = datetime.datetime(2016, 1, 2, 10, 20, 30, 123456)
        self.text2 = 'This is another text.'
        with mysql_ext.db('test') as query:
            query('CREATE TABLE texts (id INT AUTO_INCREMENT PRIMARY KEY, date DATETIME(6), text TEXT)')

    def basic_insert(self, query: mysql_ext.db):
        query.insert('texts', date=self.date1, text=self.text1)
        query.insert('texts', date=self.date2, text=self.text2)

    def tearDown(self):
        with mysql_ext.db('test') as query:
            query('DROP TABLE texts')

    def test_select(self):
        with mysql_ext.db('test') as query:
            self.basic_insert(query)
            self.assertListEqual([self.text1, self.text2], query.select('texts', 'text'))
            self.assertListEqual([self.text1], query.select('texts', 'text', date=self.date1))
            self.assertListEqual([self.text2], query.select('texts', 'text', date=self.date2))

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

    def test_update(self):
        with mysql_ext.db('test') as query:
            self.basic_insert(query)
            text = "hello world"
            self.assertEqual(1, query.update('texts', 2, text=text))
            self.assertEqual(0, query.update('texts', dict(text=text), text=text))
            self.assertListEqual([self.text1, text], query.select('texts', 'text'))

    def test_delete(self):
        with mysql_ext.db('test') as query:
            self.basic_insert(query)
            self.assertEqual(1, query.delete('texts', text=self.text1))
            self.assertListEqual([2], query.select('texts'))