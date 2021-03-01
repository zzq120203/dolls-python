from enum import Enum
from typing import List, Tuple

from redis import Redis, ConnectionPool
from redis.sentinel import Sentinel


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

        if redis_mode == RedisMode.SENTINEL:
            if urls is not List or urls is not Tuple:
                raise TypeError("url : [('hostname', 6379),('hostname', 6378)]")
            self.__pool = Sentinel(urls, socket_timeout=self.timeout, db=self.db)
            self.__conn = self.__pool.master_for(self.master_name, socket_timeout=self.timeout)
        elif redis_mode == RedisMode.CLUSTER:
            pass
        elif redis_mode == RedisMode.STANDALONE:
            if urls is not List or urls is not Tuple:
                raise TypeError("url : ('hostname', 6379)")
            hostname, port = urls
            self.__pool = ConnectionPool(host=hostname, port=port, socket_timeout=self.timeout, db=self.db)
            self.__conn = Redis(connection_pool=self.__pool)
        else:
            raise TypeError('redis mode err')

    def connection(self):
        return self.__conn

    def database(self):
        return self.__conn

    def graph(self):
        """
        RedisGraph api
        :return:
        """
        return #Graph(self.__conn)

    def json(self):
        """
        RedisJson api
        :return:
        """
        return #Json(self.__conn)

    def search(self):
        """
        RediSearch api
        :return:
        """
        return #Search(self.__conn)
