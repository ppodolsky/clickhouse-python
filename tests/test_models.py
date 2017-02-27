import datetime
import unittest

import pytz
from clickhouse.engines import MergeTree
from clickhouse.fields import (DateField, DateTimeField, Float32Field,
                               Int32Field, StringField)
from clickhouse.models import Model


class ModelTestCase(unittest.TestCase):

    def test_defaults(self):
        # Check that all fields have their explicit or implicit defaults
        instance = SimpleModel()
        self.assertEqual(instance.date_field, datetime.date(1970, 1, 1))
        self.assertEqual(instance.datetime_field, datetime.datetime(1970, 1, 1, tzinfo=pytz.utc))
        self.assertEqual(instance.str_field, 'dozo')
        self.assertEqual(instance.int_field, 17)
        self.assertEqual(instance.float_field, 0)

    def test_assignment(self):
        # Check that all fields are assigned during construction
        kwargs = dict(
            date_field=datetime.date(1973, 12, 6),
            datetime_field=datetime.datetime(2000, 5, 24, 10, 22, tzinfo=pytz.utc),
            str_field='aloha',
            int_field=-50,
            float_field=3.14
        )
        instance = SimpleModel(**kwargs)
        for name, value in kwargs.items():
            self.assertEqual(kwargs[name], getattr(instance, name))

    def test_assignment_error(self):
        # Check non-existing field during construction
        with self.assertRaises(AttributeError):
            instance = SimpleModel(int_field=7450, pineapple='tasty')
        # Check invalid field values during construction
        with self.assertRaises(ValueError):
            instance = SimpleModel(int_field='nope')
        with self.assertRaises(ValueError):
            instance = SimpleModel(date_field='nope')
        # Check invalid field values during assignment
        instance = SimpleModel()
        with self.assertRaises(ValueError):
            instance.datetime_field = datetime.timedelta(days=1)

    def test_string_conversion(self):
        # Check field conversion from string during construction
        instance = SimpleModel(date_field='1973-12-06', int_field='100', float_field='7')
        self.assertEqual(instance.date_field, datetime.date(1973, 12, 6))
        self.assertEqual(instance.int_field, 100)
        self.assertEqual(instance.float_field, 7)
        # Check field conversion from string during assignment
        instance.int_field = '99'
        self.assertEqual(instance.int_field, 99)


class SimpleModel(Model):

    date_field = DateField()
    datetime_field = DateTimeField()
    str_field = StringField(default='dozo')
    int_field = Int32Field(default=17)
    float_field = Float32Field()

    engine = MergeTree('date_field', ('int_field', 'date_field'))
