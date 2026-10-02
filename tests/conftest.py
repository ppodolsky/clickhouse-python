import os
from uuid import uuid4

import pytest

from clickhouse.database import Database


def pytest_addoption(parser):
    parser.addoption(
        '--run-integration', action='store_true', help='Run ClickHouse integration tests'
    )


def pytest_collection_modifyitems(config, items):
    if config.getoption('--run-integration'):
        return
    skip = pytest.mark.skip(reason='Use --run-integration with a running ClickHouse server')
    for item in items:
        if item.get_closest_marker('integration'):
            item.add_marker(skip)


@pytest.fixture(autouse=True)
def integration_database(request):
    if not request.node.get_closest_marker('integration'):
        yield
        return
    database = Database(
        os.getenv('CLICKHOUSE_HOST', 'localhost:8123'),
        'clickhouse_test_' + uuid4().hex,
        username=os.getenv('CLICKHOUSE_USER', 'default'),
        password=os.getenv('CLICKHOUSE_PASSWORD', ''),
        timeout=10,
        wait_for_databases_init_time=10,
    )
    request.instance.database = database
    try:
        for model in request.instance.integration_models:
            database.create_table(model)
        yield
    finally:
        try:
            database.drop_database()
        finally:
            database.close()
