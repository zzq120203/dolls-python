import unittest

from redisearch import IndexDefinition, TextField
from redisgraph import Node, Edge
from rejson import Path

from dolls.pydis import RedisPool


class TestConnection(unittest.TestCase):
    def test_standalone(self):
        pool = RedisPool(urls=("localhost", 6379))
        self.assertIsNotNone(pool)
        self.assertIsNotNone(pool.database())
        pool.close()

    def test_sentinel(self):
        pool = RedisPool(urls=[("localhost", 6379)], redis_mode=RedisMode.SENTINEL, master_name="mymaster")
        self.assertIsNotNone(pool)
        self.assertIsNotNone(pool.database())
        pool.close()

    # def test_cluster(self):
    #     pool = RedisPool(urls=("localhost", 6379), redis_mode=RedisMode.CLUSTER)
    #     self.assertIsNotNone(pool)
    #     self.assertIsNotNone(pool.database())
    #     pool.close()

    def test_api(self):
        pool = RedisPool(urls=("localhost", 6379))
        db = pool.database()
        db.set("key1", "value1")
        self.assertEqual("value1", db.get("key1"))
        pool.close()


class TestRedisGraph(unittest.TestCase):
    def test_graph(self):
        pool = RedisPool(urls=("localhost", 6379))
        graph = pool.graph("test")
        self.assertIsNotNone(graph)

    def test_node(self):
        pool = RedisPool(urls=("localhost", 6379))
        graph = pool.graph("test")
        john = Node(label='person', properties={'name': 'John Doe', 'age': 33, 'gender': 'male', 'status': 'single'})
        graph.add_node(john)

        japan = Node(label='country', properties={'name': 'Japan'})
        graph.add_node(japan)

        edge = Edge(john, 'visited', japan, properties={'purpose': 'pleasure'})
        graph.add_edge(edge)

        graph.commit()

    def test_query(self):
        pool = RedisPool(urls=("localhost", 6379))
        graph = pool.graph("test")
        query = """MATCH (p:person)-[v:visited {purpose:"pleasure"}]->(c:country)
        		   RETURN p.name, p.age, v.purpose, c.name"""

        result = graph.query(query)

        result.pretty_print()


class TestRedisJson(unittest.TestCase):
    def test_json(self):
        pool = RedisPool(urls=("localhost", 6379))
        json = pool.json()
        self.assertIsNotNone(json)

    def test_set(self):
        pool = RedisPool(urls=("localhost", 6379))
        json = pool.json()
        json.jsonset("test", Path.rootPath(), "123qweasdzxc")
        self.assertEqual(json.jsontype("test", Path.rootPath()), 'string')

    def test_get_field(self):
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

        self.assertEqual(json.jsonget('obj', Path('.truth.coord')), 'out there')


class TestRedisSearch(unittest.TestCase):
    def test_search(self):
        pool = RedisPool(urls=("localhost", 6379))
        search = pool.search()
        self.assertIsNotNone(search)
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
        self.assertEqual(res.total, 1)
        self.assertEqual(res.docs[0].title, "RediSearch")



if __name__ == '__main__':
    unittest.main()