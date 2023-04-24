# Dolls 
## Redis API

集成 [redis-py](https://github.com/andymccurdy/redis-py), [RediSearch](https://github.com/RediSearch/redisearch-py)
, [RedisGraph](https://github.com/RedisGraph/redisgraph-py), [RedisJson](https://github.com/RedisJSON/redisjson-py)

### 1.redis connection

单机模式
```python
from dolls import redis

pool = redis.from_url(url="redis://:pass@localhost:6379/0")
db = pool.database()
db.set("key1", "value1")
```
sentinel模式
```python
from dolls import redis
# scheme: redis++sentinel or rediss
pool = redis.from_url(url="redis++sentinel://:pass@localhost:6379;localhost:6380/0")
db = pool.database()
db.set("key1", "value1")
```
cluster模式
```python
from dolls import redis
# scheme: redis++cluster or redisc
pool = redis.from_url(url="redis++cluster://:pass@localhost:6379;localhost:6380/0")
db = pool.database()
db.set("key1", "value1")
```

### 2.redis graph
> 参考redisgraph-py 文档

```python
from dolls import redis
from redisgraph import Node

pool = redis.from_url(url="redis://:pass@localhost:6379/0")
graph = pool.graph("index_name")
node1 = Node()
graph.add_node(node1)
```

### 3.redis json
> 参考rejson 文档

```python
from dolls import redis
from rejson import Path

pool = redis.from_url(url="redis://:pass@localhost:6379/0")
json = pool.json()
obj = {
    'answer': 42,
    'arr': [None, True, 3.14],
    'truth': {
        'coord': 'out there'
    }
}
json.jsonset('obj', Path.rootPath(), obj)
```

### 4.redis search
> 参考 redisearch-py 文档

```python
from dolls import redis
from redisearch import TextField, IndexDefinition, Query

pool = redis.from_url(url="redis://:pass@localhost:6379/0")
search = pool.search("index_name")

# IndexDefinition is available for RediSearch 2.0+
definition = IndexDefinition(prefix=['doc:', 'article:'])

# Creating the index definition and schema
search.create_index((TextField("title", weight=5.0), TextField("body")), definition=definition)

search.hset('doc:1',
            mapping={
                'title': 'RediSearch',
                'body': 'Redisearch impements a search engine on top of redis'
            })
# Simple search
res = search.search("search engine")

# the result has the total number of results, and a list of documents
print(res.total)
print(res.docs[0].title)

# Searching with complex parameters:
q = Query("search engine").verbatim().no_content().with_scores().paging(0, 5)
res = search.search(q)
```

### 5.关闭连接

```python
from dolls import redis

pool = redis.from_url(url="redis://:pass@localhost:6379/0")
pool.close()
```

## DB Api

### 1. 创建连接

```python
from dolls import dbapi

client = dbapi.from_url(url="mysql://root:123456@localhost:6379/db")
```

### 2. 创建table

```python
table_name = "test1"
fields = [{"name": "column1", "type": "int", "primary_key": True, "comment": None}]

table = client.table(table_name, fields)

```

### 3. 调用table

```python

# 插入数据
async with client.engine.connect() as conn:
    await conn.execute(table.insert(), [{}])

```

## Gos Api
### 1. 创建连接

```python
from dolls import gos

client = gos.from_url(url="mysql://root:123456@localhost:6379/db")
```

