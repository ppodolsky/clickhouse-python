load('//tools/package:python.bzl', 'python_package', 'python_venv')
load('//tools/testing:python.bzl', 'python_test')


python_package(
    name = 'clickhouse-wheel',
    package_name = 'clickhouse',
    version = '0.1.5',
)

python_venv(
    name = 'venv',
    wheels = ['//library/python-commons:izihawa-commons-wheel', ':clickhouse-wheel'],
)

python_test(
    name = 'clickhouse-tests',
    venv = ':venv',
    tests = 'tests',
)

