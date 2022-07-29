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
    def __init__(self, urls,
                 username=None,
                 password=None,
                 redis_mode=RedisMode.STANDALONE,
                 timeout=5,
                 master_name=None,
                 db=0,
                 decode_responses=True,
                 **kwargs):
        """

        :param urls: redis 地址 ('hostname', 6379)
            或 [('hostname', 6379),('hostname', 6378)]
            或 [{"host": "127.0.0.1", "port": "7000"}, {"host": "127.0.0.1", "port": "7001"}]
        :param password: auth
        :param redis_mode: @see RedisMode
        :param timeout:
        :param master_name:
        :param db:
        :param decode_responses:
        :param kwargs:
        """
        self.urls = urls
        self.username = username
        self.password = password
        self.redis_mode = redis_mode
        self.timeout = timeout
        self.master_name = master_name
        self.db = db

        self.__conn = None
        self.__pool = None
        self.__cluster = None
        if self.redis_mode == RedisMode.SENTINEL or self.redis_mode == 1:
            if isinstance(urls, str):
                urls = [url.split(":") for url in urls.split(";")]
            if not isinstance(urls, List) and not isinstance(urls, Tuple):
                raise TypeError("url : [('hostname', 6379),('hostname', 6378)]")
            sentinel = Sentinel(urls, socket_timeout=self.timeout, db=self.db, username=username,
                                password=self.password, decode_responses=decode_responses, **kwargs)
            self.__pool = SentinelConnectionPool(master_name, sentinel, username=username, password=self.password,
                                                 decode_responses=decode_responses, db=self.db, **kwargs)
        elif self.redis_mode == RedisMode.CLUSTER or self.redis_mode == 2:
            if isinstance(urls, str):
                def addr(url):
                    _host, _port = url.split(":")
                    return {"host": _host, "port": _port}

                urls = [addr(url) for url in urls.split(";")]

            if not isinstance(urls, List) and not isinstance(urls, Tuple):
                raise TypeError('url : [{"host": "127.0.0.1", "port": "7000"}, {"host": "127.0.0.1", "port": "7001"}]')
            from rediscluster import RedisCluster
            self.__cluster = RedisCluster(startup_nodes=urls, decode_responses=decode_responses,
                                          socket_timeout=self.timeout, db=self.db, username=username,
                                          password=self.password, **kwargs)
        elif self.redis_mode == RedisMode.STANDALONE or self.redis_mode == 0:
            if isinstance(urls, str):
                if ";" in urls:
                    raise TypeError("url = 'hostname:port' or mode = RedisMode.CLUSTER")
                urls = urls.split(":")
            if not isinstance(urls, List) and not isinstance(urls, Tuple):
                raise TypeError("url : ('hostname', 6379)")
            hostname, port = urls
            self.__pool = ConnectionPool(host=hostname, port=port, socket_timeout=self.timeout,
                                         password=self.password, db=self.db, username=username,
                                         decode_responses=decode_responses, **kwargs)
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

    def database(self) -> Redis:
        if self.redis_mode == RedisMode.CLUSTER or self.redis_mode == 2:
            return self.__cluster
        else:
            return Database(self.__pool)

    def graph(self, name):
        """
        RedisGraph api @see https://github.com/RedisGraph/redisgraph-py
        pip install redisgraph
        :param name:
        :return:
        """
        if self.redis_mode == RedisMode.CLUSTER or self.redis_mode == 2:
            raise NotImplementedError("cluster is not supported")
        if not self.__conn:
            self.__connection()

        from redisgraph import Graph
        return Graph(name, self.__conn)

    def json(self):
        """
        RedisJson api @see https://github.com/RedisJSON/redisjson-py
        pip install rejson
        :return:
        """
        if self.redis_mode == RedisMode.CLUSTER or self.redis_mode == 2:
            raise NotImplementedError("cluster is not supported")

        from dolls.redis.pulgins.json import Json
        return Json(self.__pool)

    def search(self, index_name):
        """
        RediSearch api @see https://github.com/RediSearch/redisearch-py
        pip install redisearch
        :return:
        """
        if self.redis_mode == RedisMode.CLUSTER or self.redis_mode == 2:
            raise NotImplementedError("cluster is not supported")

        from dolls.redis.pulgins.search import Search
        return Search(index_name, self.__pool)

    def table(self, table_name):
        """
        :param table_name:
        :return:
        """
        if self.redis_mode == RedisMode.CLUSTER or self.redis_mode == 2:
            raise NotImplementedError("cluster is not supported")

        from dolls.redis.pulgins.table import Table
        return Table(self.search(table_name), table_name)

    def close(self):
        """
        关闭连接池和当前使用的连接
        :return:
        """
        if self.__pool:
            self.__pool.disconnect()
        if self.__cluster:
            self.__cluster.close()
