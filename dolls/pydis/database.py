from redis import Redis


class Database(Redis):
    def __init__(self, connection_pool):
        """
        :param connection_pool:
        """
        super().__init__(connection_pool=connection_pool)



