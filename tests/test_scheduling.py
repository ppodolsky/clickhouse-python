from concurrent.futures import ThreadPoolExecutor

import pytest

from clickhouse._scheduling import ExponentialBackoff, HostManager, NoAvailableHostsException


def test_priority_failover_and_recovery(monkeypatch):
    clock = [100.0]
    monkeypatch.setattr('clickhouse._scheduling.time.monotonic', lambda: clock[0])
    hosts = HostManager()
    hosts.add(1, 'primary')
    hosts.add(1, 'peer')
    hosts.add(2, 'backup')
    assert hosts.hosts_set() == {'primary', 'peer', 'backup'}
    assert {hosts.get() for _ in range(20)} <= {'primary', 'peer'}
    hosts.cooldown('primary', 5)
    assert hosts.get() == 'peer'
    hosts.cooldown('peer', 10)
    assert hosts.get() == 'backup'
    hosts.cooldown('backup', 20)
    with pytest.raises(NoAvailableHostsException):
        hosts.get()
    clock[0] += 5
    assert hosts.get() == 'primary'


def test_empty_topology():
    with pytest.raises(NoAvailableHostsException):
        HostManager().get()


def test_backoff_is_per_host_capped_and_resettable():
    backoff = ExponentialBackoff(1, 2, 8)
    assert [backoff('a') for _ in range(6)] == [1, 2, 4, 8, 8, 8]
    assert backoff('b') == 1
    backoff.reset('a')
    assert backoff('a') == 1


def test_backoff_concurrent_updates():
    backoff = ExponentialBackoff(1, 2, 512, threaded=True)
    with ThreadPoolExecutor(max_workers=4) as executor:
        delays = list(executor.map(backoff, ['host'] * 10))
    assert sorted(delays) == [2**i for i in range(10)]
