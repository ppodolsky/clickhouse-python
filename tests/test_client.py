from unittest.mock import Mock, patch

import pytest
import requests

from clickhouse.database import Database, NoAvailableHostsException
from clickhouse.fields import StringField
from clickhouse.models import Model
from clickhouse.utils import parse_tsv


class Message(Model):
    text = StringField()


@pytest.fixture
def client():
    with patch('clickhouse.database.requests.Session') as session:
        session.return_value.post.return_value = Mock(status_code=200)
        database = Database('http://localhost:8123', 'test')
        session.return_value.post.reset_mock()
        yield database
        database.close()


@pytest.mark.parametrize('factory', [list, iter, lambda values: (value for value in values)])
def test_insert_accepts_iterables(client, factory):
    client.insert(factory([Message(text='hello')]))
    assert client._requests_session.post.call_args.kwargs['data'] == (
        b'INSERT INTO `test`.`message` FORMAT TabSeparated\nhello'
    )


def test_empty_iterator_does_not_send(client):
    client.insert(iter([]))
    client._requests_session.post.assert_not_called()


def test_buffer_and_explicit_flush(client):
    client._buffer_size = 2
    client.insert([Message(text='a'), Message(text='b')])
    client._requests_session.post.assert_not_called()
    client.flush()
    assert client._requests_session.post.call_count == 1
    client.flush()
    assert client._requests_session.post.call_count == 1


def test_query_failover(client):
    client._load_hosts({2: ['backup:8123']})
    response = Mock(status_code=200)
    client._requests_session.post.side_effect = [requests.ConnectionError('offline'), response]
    assert client.query('SELECT 1') is response
    assert [call.args[0] for call in client._requests_session.post.call_args_list] == [
        'http://localhost:8123',
        'http://backup:8123',
    ]


def test_all_hosts_unavailable(client):
    client._requests_session.post.side_effect = requests.ConnectionError('offline')
    with pytest.raises(NoAvailableHostsException):
        client.query('SELECT 1')


@pytest.mark.parametrize('line', ['', b'', '\n', b'\n'])
def test_empty_string_tsv(line):
    assert parse_tsv(line) == ['']


def test_unicode_tsv_roundtrip():
    original = Message(text='אבגד é\n\t\\')
    assert Message.from_tsv(original.to_tsv().encode()).text == original.text
