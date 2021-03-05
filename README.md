# Redis API
集成 [redis-py](https://github.com/andymccurdy/redis-py), [RediSearch](https://github.com/RediSearch/redisearch-py), [RedisGraph](https://github.com/RedisGraph/redisgraph-py), [RedisJson](https://github.com/RedisJSON/redisjson-py)

1.redis connection
```python
from dolls.pydis import RedisPool

pool = RedisPool(urls=("localhost", 6379))
db = pool.database()
db.set("key1", "value1")
```

```python
from dolls.pydis import RedisPool, RedisMode

pool = RedisPool(urls=("localhost", 6379), redis_mode=RedisMode.SENTINEL, master_name="master")
db = pool.database()
db.set("key1", "value1")
```

2.redis graph
> 参考redisgraph-py 文档
```python
from dolls.pydis import RedisPool
from redisgraph import Node

pool = RedisPool(urls=("localhost", 6379))
graph = pool.graph("index_name")
node1 = Node()
graph.add_node(node1)
```

3.redis json
>参考rejson 文档
```python
from dolls.pydis import RedisPool
from rejson import Path

pool = RedisPool(urls=("localhost", 6379))
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

4.redis search
> 参考 redisearch-py 文档
```python
from dolls.pydis import RedisPool
from redisearch import TextField, IndexDefinition, Query

pool = RedisPool(urls=("localhost", 6379))
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
5.关闭连接
```python
from dolls.pydis import RedisPool

pool = RedisPool(urls=("localhost", 6379))
pool.close()
```