from enum import Enum

import pytest

from clickhouse.fields import ArrayField, DateField, Enum8Field, Int32Field, StringField, UInt8Field
from clickhouse.models import Model


class Status(Enum):
    ready = 1
    done = 2


class ValidationModel(Model):
    title = StringField()
    quantity = Int32Field()
    day = DateField()
    status = Enum8Field(Status)
    counts = ArrayField(UInt8Field())


@pytest.mark.parametrize(
    ('field_name', 'value'),
    [
        ('title', 42),
        ('quantity', 'invalid'),
        ('quantity', 2**31),
        ('day', 'invalid'),
        ('day', '2100-01-01'),
        ('status', 'missing'),
        ('counts', ['invalid']),
        ('counts', [256]),
    ],
)
@pytest.mark.parametrize('assignment', [False, True])
def test_validation_error_identifies_model_and_field(field_name, value, assignment):
    instance = ValidationModel()
    previous = getattr(instance, field_name)
    with pytest.raises(ValueError) as error:
        if assignment:
            setattr(instance, field_name, value)
        else:
            ValidationModel(**{field_name: value})

    message = str(error.value)
    assert f'ValidationModel.{field_name}' in message
    assert isinstance(error.value.__cause__, ValueError)
    assert str(error.value.__cause__) in message
    assert getattr(instance, field_name) == previous


def test_tsv_error_identifies_column():
    with pytest.raises(ValueError, match=r'ValidationModel\.quantity'):
        ValidationModel.from_tsv('hello\tinvalid', ['title', 'quantity'])


def test_inherited_field_error_identifies_concrete_model():
    class ChildModel(ValidationModel):
        pass

    with pytest.raises(ValueError, match=r'ChildModel\.quantity'):
        ChildModel(quantity='invalid')


def test_invalid_default_identifies_field():
    class InvalidDefaultModel(Model):
        quantity = Int32Field(default='invalid')

    with pytest.raises(ValueError, match=r'InvalidDefaultModel\.quantity'):
        InvalidDefaultModel()
