from enum import Enum
from typing import List, Tuple

from redis import Redis, ConnectionPool
from redis.sentinel import Sentinel

from .database import Database
from .json import Json
from .search import Search


class RedisMode(Enum):
    STANDALONE = 0
    SENTINEL = 1
    CLUSTER = 2


class RedisPool(object):
    def __init__(self, urls, password=None, redis_mode=RedisMode.STANDALONE, timeout=5, master_name=None, db=0):
        """

        :param urls: redis 地址 ('hostname', 6379) 或 [('hostname', 6379),('hostname', 6378)]
        :param password: auth
        :param redis_mode: @see RedisMode
        :param timeout:
        :param master_name:
        :param db:
        """
        self.urls = urls
        self.password = password
        self.redis_mode = redis_mode
        self.timeout = timeout
        self.master_name = master_name
        self.db = db

        self.__conn = None
        if self.redis_mode == RedisMode.SENTINEL:
            if urls is not List or urls is not Tuple:
                raise TypeError("url : [('hostname', 6379),('hostname', 6378)]")
            self.__pool = Sentinel(urls, socket_timeout=self.timeout, db=self.db)
        elif self.redis_mode == RedisMode.CLUSTER:
            pass
        elif self.redis_mode == RedisMode.STANDALONE:
            if urls is not List or urls is not Tuple:
                raise TypeError("url : ('hostname', 6379)")
            hostname, port = urls
            self.__pool = ConnectionPool(host=hostname, port=port, socket_timeout=self.timeout, db=self.db)
        else:
            raise TypeError('redis mode err')

    def __connection(self) -> Redis:
        if not self.__conn:
            if self.redis_mode == RedisMode.SENTINEL:
                self.__conn = self.__pool.master_for(self.master_name, socket_timeout=self.timeout)
            elif self.redis_mode == RedisMode.CLUSTER:
                pass
            elif self.redis_mode == RedisMode.STANDALONE:
                self.__conn = Redis(connection_pool=self.__pool)
            else:
                raise TypeError('redis mode err')
        return self.__conn

    def database(self) -> Database:
        return Database(self.__pool)

    def graph(self, name=""):
        """
        RedisGraph api @see https://github.com/RedisGraph/redisgraph-py
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
        :return:
        """
        return Json(self.__pool)

    def search(self, index_name=""):
        """
        RediSearch api @see https://github.com/RediSearch/redisearch-py
        :return:
        """
        if not self.__conn:
            self.__connection()
        return Search(index_name, self.__conn)
