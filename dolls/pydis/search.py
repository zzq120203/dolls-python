from redis import Redis
from redisearch import Client


class Search(Client):
    def __init__(self, index_name, connection_pool):
        super().__init__(index_name)
        self.redis = Redis(connection_pool)