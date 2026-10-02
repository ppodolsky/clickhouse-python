"""Host selection and backoff without external scheduling dependencies."""

import random
import time
from threading import Lock


class NoAvailableHostsException(Exception):
    """No configured host is currently outside its cooldown period."""


class HostManager:
    def __init__(self, threaded=False):
        # Always synchronize state; retain the constructor option used by Database.
        self._lock = Lock()
        self._hosts = {}
        self._cooldowns = {}

    def add(self, priority, host):
        with self._lock:
            self._hosts[host] = priority

    def hosts_set(self):
        with self._lock:
            return set(self._hosts)

    def cooldown(self, host, seconds):
        with self._lock:
            self._cooldowns[host] = time.monotonic() + seconds

    def get(self):
        with self._lock:
            now = time.monotonic()
            available = [host for host in self._hosts if self._cooldowns.get(host, 0) <= now]
            if not available:
                raise NoAvailableHostsException('All hosts are unavailable or cooling down')
            priority = min(self._hosts[host] for host in available)
            return random.choice([host for host in available if self._hosts[host] == priority])


class ExponentialBackoff:
    def __init__(self, initial=1, multiplier=2, maximum=512, threaded=False):
        self._initial = initial
        self._multiplier = multiplier
        self._maximum = maximum
        self._delays = {}
        self._lock = Lock()

    def __call__(self, host):
        with self._lock:
            delay = self._delays.get(host, self._initial)
            self._delays[host] = min(delay * self._multiplier, self._maximum)
            return delay

    def reset(self, host):
        with self._lock:
            self._delays.pop(host, None)
