from enum import Enum
from typing import List, Tuple

from redis import Redis, ConnectionPool
from redis.sentinel import Sentinel, SentinelConnectionPool

from .database import Database


class RedisMode(Enum):
    STANDALONE = 0
    SENTINEL = 1
    CLUSTER = 2


class RedisPool(object):
    def __init__(self, urls, password=None, redis_mode=RedisMode.STANDALONE, timeout=5, master_name=None, db=0,
                 decode_responses=True):
        """

        :param urls: redis 地址 ('hostname', 6379) 或 [('hostname', 6379),('hostname', 6378)]
        :param password: auth
        :param redis_mode: @see RedisMode
        :param timeout:
        :param master_name:
        :param db:
        :param decode_responses:
        """
        self.urls = urls
        self.password = password
        self.redis_mode = redis_mode
        self.timeout = timeout
        self.master_name = master_name
        self.db = db

        self.__conn = None
        self.__pool = None
        if self.redis_mode == RedisMode.SENTINEL or self.redis_mode == 1:
            if not isinstance(urls, List) and not isinstance(urls, Tuple):
                raise TypeError("url : [('hostname', 6379),('hostname', 6378)]")
            sentinel = Sentinel(urls, socket_timeout=self.timeout, db=self.db,
                                password=self.password, decode_responses=decode_responses)
            self.__pool = SentinelConnectionPool(master_name, sentinel)
        elif self.redis_mode == RedisMode.CLUSTER or self.redis_mode == 2:
            raise NotImplementedError("cluster is not supported")
        elif self.redis_mode == RedisMode.STANDALONE or self.redis_mode == 0:
            if not isinstance(urls, List) and not isinstance(urls, Tuple):
                raise TypeError("url : ('hostname', 6379)")
            hostname, port = urls
            self.__pool = ConnectionPool(host=hostname, port=port, socket_timeout=self.timeout,
                                         password=self.password, db=self.db,
                                         decode_responses=decode_responses)
        else:
            raise TypeError('redis mode err')

    def __connection(self) -> Redis:
        if not self.__conn:
            if self.redis_mode == RedisMode.SENTINEL or self.redis_mode == RedisMode.STANDALONE:
                self.__conn = Redis(connection_pool=self.__pool)
            elif self.redis_mode == RedisMode.CLUSTER:
                pass
            else:
                raise TypeError('redis mode err')
        return self.__conn

    def database(self) -> Database:
        return Database(self.__pool)

    def graph(self, name):
        """
        RedisGraph api @see https://github.com/RedisGraph/redisgraph-py
        pip install redisgraph
        :param name:
        :return:
        """
        if not self.__conn:
            self.__connection()
        import redisgraph
        return redisgraph.Graph(name, self.__conn)

    def json(self):
        """
        RedisJson api @see https://github.com/RedisJSON/redisjson-py
        pip install rejson
        :return:
        """
        from .json import Json
        return Json(self.__pool)

    def search(self, index_name):
        """
        RediSearch api @see https://github.com/RediSearch/redisearch-py
        pip install redisearch
        :return:
        """
        from .search import Search
        return Search(index_name, self.__pool)

    def table(self, table_name):
        """
        :param table_name:
        :return:
        """
        from .table import Table
        return Table(self.search(table_name), table_name)

    def close(self):
        """
        关闭连接池和当前使用的连接
        :return:
        """
        if self.__pool:
            self.__pool.disconnect()
