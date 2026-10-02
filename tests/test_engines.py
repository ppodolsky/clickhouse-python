import pytest

from clickhouse.engines import (
    CollapsingMergeTree,
    MergeTree,
    ReplacingMergeTree,
    SummingMergeTree,
)


@pytest.mark.parametrize(
    ('engine_class', 'options', 'parameters'),
    [
        (MergeTree, {}, ''),
        (CollapsingMergeTree, {'sign_col': 'sign'}, 'sign'),
        (SummingMergeTree, {'summing_cols': ('amount', 'count')}, '(amount, count)'),
        (SummingMergeTree, {}, ''),
        (ReplacingMergeTree, {'version_col': 'version'}, 'version'),
        (ReplacingMergeTree, {}, ''),
    ],
)
@pytest.mark.parametrize('replicated', [False, True])
def test_modern_engine_sql(engine_class, options, parameters, replicated):
    if replicated:
        options = dict(options, replica_table_path='/clickhouse/table', replica_name='replica-1')
        parameters = "'/clickhouse/table', 'replica-1'" + (', ' + parameters if parameters else '')
    engine = engine_class('day', ('id', 'day'), index_granularity=4096, **options)
    name = ('Replicated' if replicated else '') + engine_class.__name__
    assert engine.create_table_sql() == (
        f'{name}({parameters})\n'
        'PARTITION BY toYYYYMM(day)\n'
        'ORDER BY (id, day)\n'
        'SETTINGS index_granularity = 4096'
    )


def test_sampling_clause():
    sql = MergeTree('day', ('id',), sampling_expr='id').create_table_sql()
    assert '\nSAMPLE BY id\nSETTINGS ' in sql


@pytest.mark.parametrize('options', [{'replica_table_path': '/table'}, {'replica_name': 'replica'}])
def test_replication_requires_path_and_name(options):
    with pytest.raises(ValueError, match='supplied together'):
        MergeTree('day', ('id',), **options)
