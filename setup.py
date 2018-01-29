# -*- coding: utf-8 -*-


from setuptools import (
    find_packages,
    setup,
)

if __name__ == '__main__':
    setup(
        name='clickhouse',
        version='0.1.7',
        author='ppodolsky',
        author_email='ppodolsky@yandex-team.ru',

        url='https://github.com/ppodolsky/clickhouse-python',
        description="""A Python library for working with the ClickHouse database""",

        classifiers=[
            "Intended Audience :: Developers",
            "Intended Audience :: System Administrators",
            "License :: OSI Approved :: Python Software Foundation License",
            "Operating System :: OS Independent",
            "Programming Language :: Python",
            "Programming Language :: Python :: 2.7",
            "Programming Language :: Python :: 3.4",
            "Topic :: Software Development :: Libraries :: Python Modules",
            "Topic :: Database"
        ],

        packages=find_packages(exclude=('tests', 'tests.*')),
        install_requires=[
            'pytz',
            'requests',
            'setuptools',
            'six',
            'enum34',
            'izihawa-commons >= 0.0.10',
        ]
    )
