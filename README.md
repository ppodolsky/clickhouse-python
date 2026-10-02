# clickhouse-python

A synchronous Python client and small ORM for ClickHouse's HTTP interface. Define
models, insert batches, select typed rows, and route requests between hosts using
priorities and failure cooldowns.

## Requirements and installation

Version 0.2.0 supports **Python 3.11–3.14**. Python 2 and older Python 3
versions are no longer supported. Runtime dependencies are `requests` and `pytz`;
`enum34`, `six`, and `izihawa-commons` are no longer required.

Install the package from PyPI:

```bash
python -m pip install clickhouse==0.2.0
```

To install from a source checkout, run `python -m pip install .`. The distribution
and import package are both named `clickhouse`.

A running ClickHouse server with its HTTP interface enabled (usually port 8123)
is needed for database operations. CI exercises ClickHouse 25.8. The ORM exposes
a limited set of field types and the existing MergeTree engine API; it does not
cover every feature of modern ClickHouse. MergeTree constructors retain their
existing arguments but now generate `PARTITION BY`, `ORDER BY`, and `SETTINGS`
clauses. Monthly partitioning by `date_col` is preserved; existing tables are
not migrated.

## Quick start

```python
from clickhouse import engines, fields, models
from clickhouse.database import Database


class Person(models.Model):
    _table_name = 'person'

    first_name = fields.StringField()
    last_name = fields.StringField()
    birthday = fields.DateField()
    height = fields.Float32Field()

    engine = engines.MergeTree('birthday', ('first_name', 'last_name', 'birthday'))


db = Database('localhost:8123', 'example', timeout=10)
try:
    db.create_table(Person)
    db.insert(
        [
            Person(first_name='Ada', last_name='Lovelace', birthday='1980-12-10', height=1.65),
        ]
    )
    for person in db.select('SELECT * FROM $table ORDER BY last_name', Person):
        print(person.first_name, person.birthday)
    print(db.count(Person))
finally:
    db.close()
```

Constructing `Database` creates the database on every configured host, so the
credentials must allow `CREATE DATABASE`. Pass `username` and `password` to use
authentication. `timeout` controls normal HTTP requests;
`wait_for_databases_init_time` controls the initial database creation timeout.

`select()` returns an iterator. Supply a model to get instances of that model, or
omit it to infer an ad hoc model from the result columns. `$db` and `$table` are
identifier placeholders; `$table` requires a model. These placeholders do not
bind query values, so do not interpolate untrusted input into SQL.

## Batching and buffering

`insert()` accepts an iterable of model instances, all belonging to the same
model, and materializes it before sending. Prefer batches that fit in memory.
By default, inserts are sent immediately. Set `buffer_size` to collect instances
per model until the buffer **exceeds** that size:

```python
db = Database('localhost:8123', 'example', buffer_size=1000, threaded=True)
try:
    db.insert([Person(first_name='Grace', birthday='1985-12-09')])
    db.flush()
finally:
    db.close()
```

`flush()` sends every pending buffer. `close()` closes the HTTP session and does
**not** flush, so call `flush()` before closing when buffering is enabled. There
is no automatic timer. The `threaded` option is retained for compatibility;
buffer operations and host scheduling use locks. This is not a guarantee that
all operations on a shared Requests session are safe concurrently.

## Host topology and failover

Hosts are HTTP addresses, with `http://` optional. Include the port explicitly
when it is not 80. Topologies support these forms:

```python
# One host
single = 'localhost:8123'

# Ordered fallback: each list element has a different priority
ordered = ['ch-1:8123', 'ch-2:8123']

# Random selection among equal-priority hosts
balanced = {'ch-1:8123', 'ch-2:8123'}

# Lower numbers have higher priority
priorities = {1: ['ch-1:8123', 'ch-2:8123'], 2: ['ch-backup:8123']}

# Equivalent host-to-priority form
per_host = {'ch-1:8123': 1, 'ch-2:8123': 1, 'ch-backup:8123': 2}
```

The client randomly chooses an available host at the lowest priority number.
Failed hosts receive exponential cooldowns, starting at one second and capped
at 512 seconds. A successful query resets that host's backoff. If all hosts are
cooling down, `clickhouse.database.NoAvailableHostsException` is raised.
Database/table creation and deletion are broadcast to all hosts.

For data center preferences:

```python
from clickhouse.utils import derive_relative_topology


topology = derive_relative_topology(
    {
        'dc-1': ['ch-1.dc-1:8123', 'ch-2.dc-1:8123'],
        'dc-2': ['ch-1.dc-2:8123'],
    },
    your_dc='dc-1',
)
```

Local hosts receive priority 1 and other hosts priority 2. This is client-side
routing; it does not configure ClickHouse replication.

## Development

Packaging uses standard `pyproject.toml` metadata and the setuptools PEP 517
backend. No external monorepo build tools are needed.

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev]'
python -m pytest
ruff check .
ruff format --check .
```

Alternatively, with [uv](https://docs.astral.sh/uv/):

```bash
uv venv
uv pip install -e '.[dev]'
uv run --no-sync pytest
```

The default test run is offline and explicitly skips integration tests. To run
the complete suite against a disposable server:

```bash
docker run --rm -d --name clickhouse-python-test \
  -p 127.0.0.1:8123:8123 \
  -e CLICKHOUSE_USER=test -e CLICKHOUSE_PASSWORD=test \
  -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 \
  clickhouse/clickhouse-server:25.8

# Wait until this returns "Ok." before running tests.
curl --fail http://localhost:8123/ping
CLICKHOUSE_USER=test CLICKHOUSE_PASSWORD=test python -m pytest --run-integration

docker stop clickhouse-python-test
```

`CLICKHOUSE_HOST` defaults to `localhost:8123`; `CLICKHOUSE_USER` and
`CLICKHOUSE_PASSWORD` default to `default` and an empty password. Each integration
test creates and removes a uniquely named database. Use a test server and an
account allowed to create and drop databases. Integration tests are run through
pytest, which supplies their database fixtures.

Build and validate a source distribution and wheel:

```bash
python -m build
python -m twine check --strict dist/*
```

CI runs the complete suite on Python 3.11, 3.12, 3.13, and 3.14, checks lint and
formatting, validates distributions, and tests a clean wheel installation outside
the repository.
