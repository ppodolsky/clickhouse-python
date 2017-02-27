## Installation

```bash
pip install clickhouse
```

## Usage

### Defining Models

Models are defined in a way reminiscent of Django's ORM:

```python
from clickhouse import models, fields, engines

class Person(models.Model):
    _table_name = 'person'

    first_name = fields.StringField()
    last_name = fields.StringField()
    birthday = fields.DateField()
    height = fields.Float32Field()

    engine = engines.MergeTree('birthday', ('first_name', 'last_name', 'birthday'))
```

### Database client

The main object you are interacting with is Database:
```python
from clickhouse.database import Database

db = Database(topology, 'database_name')
```
Topology is just a special object wrapping hosts that also introduces priorities of hosts.
How to prepare a topology you can read in the next section.
If neccessary, you can specify credentials:
```python
db = Database(topology, 'database_name', username='yandexer', password='passwd')
```

ClickHouse is optimized for a bulk insert, and we've implemented embedded buffering here to avoid single inserts.
Every model (table) has its own buffer and buffer size defines how many instances of the model must be collected in buffer
before real insert. If you need more predictable inserts, you can always use ```db.flush()``` which sends all collected instances
immediately or even set ```buffer_size=0``` to flush every insert.
Buffering are disabled by default, for using it you must set an appropriate buffer_size:
```python
db = Database(topology, 'database_name', buffer_size=32)
```
The rule of thumbs to choose buffer size is to set such a size that buffer would overflow every second.
Database client can be *thread-safe*. To get thread-safety use ```threaded=True```
while creating ```Database``` object. 
You can create a separate thread to flush every second or insert in multiple threads.

### Describing topology of ClickHouse cluster

This wrapper tends to support multi DC strategies.
Topology can be described in the following format:
```python
topology = { 1: [host1, host2, host3], 2: [host4, host5, host6] }
```
where keys in the dictionary are priorities of corresponding host's lists, lesser values means higher priority.
In the topology above requests will be always sent to any of host1, host2, host3 (choosen randomly every time).
Hosts with priority 2 will be involved in action only if _all_ hosts with priority 1 fall down.

#### Go to local DC
Assuming there are two data centers DC-1 and DC-2 and code is running on a host in DC-1
```python
topology = {
    1: ['clickhouse-instance-1.dc-1.net', 'clickhouse-instance-2.dc-1.net'],
    2: ['clickhouse-instance-1.dc-2.net', 'clickhouse-instance-2.dc-2.net'],
}
```
There is a helper to produce topology in the required format from a more human readable format. Code below produces the same result as above:
```python
from clickhouse.utils import derive_relative_topology
topology = derive_relative_topology(
    {
        'dc-1': ['clickhouse-instance-1.dc-1.net', 'clickhouse-instance-2.dc-1.net'],
        'dc-2': ['clickhouse-instance-1.dc-2.net', 'clickhouse-instance-2.dc-2.net'],
    },
    your_dc='dc-1',
)
```
#### Priority list
```python
topology = ['clickhouse-instance-1.net', 'clickhouse-instance-2.net'],
```
#### Random of two
```python
topology = {'clickhouse-instance-1.net', 'clickhouse-instance-1.net'}
```
#### One host
```python
topology = 'clickhouse-instance-1.net'
```

