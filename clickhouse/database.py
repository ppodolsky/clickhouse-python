# flake8: noqa
import logging
import types
from collections import namedtuple
from string import Template
from threading import Lock

import requests
from izihawa_commons.schedule.backoff import ExponentialBackoff
from izihawa_commons.schedule.host_manager import NoAvailableHostsException
from izihawa_commons.schedule.host_manager import HostManager
from six import PY3, string_types

from .models import ModelBase
from .utils import parse_tsv, prepend_if_not

Page = namedtuple('Page', 'objects number_of_objects pages_total number page_size')


class DatabaseException(Exception):
    pass


class InconsistentConfig(DatabaseException):
    pass


error_log = logging.getLogger('clickhouse.error')


class Database(object):
    def __init__(
            self,
            topology,
            database_name,
            username=None,
            password=None,
            retries_per_host=2,
            backoff=None,
            buffer_size=0,
            timeout=None,
            requests_config=None,
            wait_for_databases_init_time=300,
            threaded=False,
    ):
        self._host_manager = HostManager(threaded=threaded)
        self._topology = topology
        self._database_name = database_name

        self._username = username
        self._password = password

        self._retries_per_host = retries_per_host

        if backoff is None:
            backoff = ExponentialBackoff(1, 2, 512, threaded=threaded)
        self._backoff = backoff

        self._buffer_size = buffer_size
        self._timeout = timeout
        self._requests_config = requests_config or {}

        self._buffer = {}
        self._buffer_lock = {}
        self._init_lock = Lock()

        self._load_hosts(self._topology)

        self._requests_pool_connections = self._requests_config.get(
            'pool_connections',
            len(self._host_manager.hosts_set())
        )
        self._requests_pool_maxsize = self._requests_config.get(
            'pool_maxsize',
            len(self._host_manager.hosts_set())
        )

        self._requests_session = requests.Session()
        a = requests.adapters.HTTPAdapter(
            pool_connections=self._requests_pool_maxsize,
            pool_maxsize=self._requests_pool_connections,
            max_retries=self._retries_per_host,
        )
        self._requests_session.mount('http://', a)

        self._requests_params = {
            'user': self._username,
            'password': self._password,
        }

        self.create_database(timeout=wait_for_databases_init_time)

    def query(
            self,
            query,
            stream_response=False,
            timeout=None,
    ):
        timeout = timeout or self._timeout
        if PY3 and isinstance(query, string_types):
            query = query.encode('utf-8')
        while True:
            target_host = self._host_manager.get()
            try:
                r = self._requests_session.post(
                    target_host,
                    params=self._requests_params,
                    data=query,
                    timeout=timeout,
                    stream=stream_response,
                )
                if r.status_code == 200:
                    self._backoff.reset(target_host)
                    return r
                else:
                    raise DatabaseException(r.text)
            except (requests.RequestException, DatabaseException) as ex:
                error_log.error(
                    'Error while requesting to %s: %s',
                    target_host,
                    ex,
                )
                bo = self._backoff(target_host)
                self._host_manager.cooldown(target_host, bo)
                error_log.error(
                    'Host %s is cooling down for %d seconds',
                    target_host,
                    bo,
                )

    def broadcast_query(self, query, ensure=True, timeout=None):
        success_counter = 0
        timeout = timeout or self._timeout
        for target_host in self._host_manager.hosts_set():
            try:
                r = self._requests_session.post(
                    target_host,
                    params=self._requests_params,
                    data=query,
                    timeout=timeout,
                )
                r.close()
                if r.status_code == 200:
                    success_counter += 1
                    continue
                else:
                    raise DatabaseException(r.text)
            except (requests.RequestException, DatabaseException) as ex:
                error_log.error(
                    'Error while requesting to %s: %s',
                    target_host,
                    str(ex),
                )
                if ensure:
                    raise ex
        return success_counter

    def flush(self):
        for model_class in self._buffer:
            self._send_instances(
                model_class,
                self._try_release_buffer(model_class, force=True),
            )

    def _init_buffer(self, model_class):
        if model_class not in self._buffer:
            with self._init_lock:
                if model_class not in self._buffer:
                    self._buffer[model_class] = []
                    self._buffer_lock[model_class] = Lock()

    def _try_release_buffer(self, model_class, force=False):
        if force or len(self._buffer[model_class]) > self._buffer_size:
            with self._buffer_lock[model_class]:
                if len(self._buffer[model_class]) > 0:
                    buffer_values = self._buffer[model_class]
                    self._buffer[model_class] = []
                    return buffer_values
                else:
                    return
        else:
            return

    def _send_instances(self, model_class, instances):
        if instances and len(instances) > 0:
            query = [
                self._substitute(
                    'INSERT INTO $table FORMAT TabSeparated',
                    model_class
                ).encode('utf-8')
            ]
            for instance in instances:
                query.append(instance.to_tsv().encode('utf-8'))
            r = self.query('\n'.encode('utf-8').join(query))
            r.close()

    def _substitute(self, query, model_class=None):
        '''
        Replaces $db and $table placeholders in the query.
        '''
        if '$' in query:
            mapping = dict(db="`%s`" % self._database_name)
            if model_class:
                mapping['table'] = "`%s`.`%s`" % (
                    self._database_name,
                    model_class.table_name()
                )
            query = Template(query).substitute(mapping)
        return query

    def create_database(self, timeout=None):
        return self.broadcast_query(
            'CREATE DATABASE IF NOT EXISTS `%s`' % self._database_name,
            ensure=True,
            timeout=timeout,
        )

    def create_table(self, model_class, timeout=None):
        return self.broadcast_query(
            model_class.create_table_sql(self._database_name),
            ensure=True,
            timeout=timeout,
        )

    def drop_table(self, model_class, timeout=None):
        return self.broadcast_query(
            model_class.drop_table_sql(self._database_name),
            ensure=True,
            timeout=timeout,
        )

    def drop_database(self, timeout=None):
        return self.broadcast_query(
            'DROP DATABASE `%s`' % self._database_name,
            ensure=True,
            timeout=timeout,
        )

    def insert(self, model_instances):
        if isinstance(model_instances, types.GeneratorType):
            model_instances = list(model_instances)
        if len(model_instances) == 0:
            return
        model_class = model_instances[0].__class__
        self._init_buffer(model_class)
        with self._buffer_lock[model_class]:
            self._buffer[model_class].extend(model_instances)
        self._send_instances(model_class, self._try_release_buffer(model_class))

    def select(self, query, model_class=None):
        query += ' FORMAT TabSeparatedWithNamesAndTypes'
        query = self._substitute(query, model_class)
        r = self.query(query, stream_response=True)
        lines = r.iter_lines()
        field_names = parse_tsv(next(lines))
        field_types = parse_tsv(next(lines))
        model_class = model_class or ModelBase.create_ad_hoc_model(zip(field_names, field_types))
        for line in lines:
            yield model_class.from_tsv(line, field_names)
        r.close()

    def count(self, model_class, conditions=None):
        query = 'SELECT count() FROM $table'
        if conditions:
            query += ' WHERE ' + conditions
        query = self._substitute(query, model_class)
        r = self.query(query)
        count_value = int(r.text) if r.text else 0
        r.close()
        return count_value

    def close(self):
        self._requests_session.close()

    def _load_hosts(self, new_hosts):
        if len(new_hosts) == 0:
            return

        if len(self._host_manager.hosts_set()) != 0 and type(new_hosts) in {set, str, list}:
            raise ValueError('Priority is not specified for new hosts')

        if isinstance(new_hosts, list):
            for priority, new_host in enumerate(new_hosts):
                self._host_manager.add(priority, prepend_if_not('http://', new_host))
        elif isinstance(new_hosts, set):
            for new_host in new_hosts:
                self._host_manager.add(1, prepend_if_not('http://', new_host))
        elif isinstance(new_hosts, dict):
            values = list(new_hosts.items())
            if isinstance(values[0][0], int) and isinstance(values[0][1], list):
                for priority, nested_new_hosts in values:
                    for new_host in nested_new_hosts:
                        self._host_manager.add(priority, prepend_if_not('http://', new_host))
            elif isinstance(values[0][0], str) and isinstance(values[0][1], int):
                for new_host, priority in values:
                    self._host_manager.add(priority, prepend_if_not('http://', new_host))
            else:
                raise InconsistentConfig(
                    'Dict object must be in format <int, list<str>> or <str, int>'
                )
        elif isinstance(new_hosts, str):
            self._host_manager.add(1, prepend_if_not('http://', new_hosts))
        else:
            raise InconsistentConfig('Passed hosts must be a list, set, string or dict object')
