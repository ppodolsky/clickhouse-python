import unittest

from clickhouse.database import Database
from clickhouse.engines import MergeTree
from clickhouse.fields import ArrayField, DateField, Enum8Field, Enum16Field
from clickhouse.models import Model

try:
    Enum  # exists in Python 3.4+
except NameError:
    from enum import Enum  # use the enum34 library instead


class EnumFieldsTest(unittest.TestCase):

    def setUp(self):
        self.database = Database('localhost:8123', 'test-db')
        self.database.create_table(ModelWithEnum)
        self.database.create_table(ModelWithEnumArray)

    def tearDown(self):
        self.database.drop_database()
        self.database.close()

    def test_insert_and_select(self):
        self.database.insert([
            ModelWithEnum(date_field='2016-08-30', enum_field=Fruit.apple),
            ModelWithEnum(date_field='2016-08-31', enum_field=Fruit.orange)
        ])
        query = 'SELECT * from $table ORDER BY date_field'
        results = list(self.database.select(query, ModelWithEnum))
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0].enum_field, Fruit.apple)
        self.assertEqual(results[1].enum_field, Fruit.orange)

    def test_ad_hoc_model(self):
        self.database.insert([
            ModelWithEnum(date_field='2016-08-30', enum_field=Fruit.apple),
            ModelWithEnum(date_field='2016-08-31', enum_field=Fruit.orange)
        ])
        query = 'SELECT * from $db.modelwithenum ORDER BY date_field'
        results = list(self.database.select(query))
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0].enum_field.name, Fruit.apple.name)
        self.assertEqual(results[0].enum_field.value, Fruit.apple.value)
        self.assertEqual(results[1].enum_field.name, Fruit.orange.name)
        self.assertEqual(results[1].enum_field.value, Fruit.orange.value)

    def test_conversion(self):
        self.assertEqual(ModelWithEnum(enum_field=3).enum_field, Fruit.orange)
        self.assertEqual(ModelWithEnum(enum_field='apple').enum_field, Fruit.apple)
        self.assertEqual(ModelWithEnum(enum_field=Fruit.banana).enum_field, Fruit.banana)

    def test_assignment_error(self):
        for value in (0, 17, 'pear', '', None, 99.9):
            with self.assertRaises(ValueError):
                ModelWithEnum(enum_field=value)

    def test_default_value(self):
        instance = ModelWithEnum()
        self.assertEqual(instance.enum_field, Fruit.apple)

    def test_enum_array(self):
        instance = ModelWithEnumArray(
            date_field='2016-08-30',
            enum_array=[Fruit.apple, Fruit.apple, Fruit.orange]
        )
        self.database.insert([instance])
        query = 'SELECT * from $table ORDER BY date_field'
        results = list(self.database.select(query, ModelWithEnumArray))
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].enum_array, instance.enum_array)


Fruit = Enum('Fruit', u'apple banana orange')


class ModelWithEnum(Model):

    date_field = DateField()
    enum_field = Enum8Field(Fruit)

    engine = MergeTree('date_field', ('date_field',))


class ModelWithEnumArray(Model):

    date_field = DateField()
    enum_array = ArrayField(Enum16Field(Fruit))

    engine = MergeTree('date_field', ('date_field',))