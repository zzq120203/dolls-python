from redis import Redis
from redisearch import Client


class Search(Redis, Client):
    def __init__(self, index_name, connection_pool):
        super().__init__(connection_pool=connection_pool)
        Client.__init__(self, index_name, conn=self)

