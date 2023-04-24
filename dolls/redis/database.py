import warnings

from redis import Redis


class Database(Redis):
    def __init__(self, connection_pool):
        """
        :param connection_pool:
        """
        super().__init__(connection_pool=connection_pool)

    def keys(self, pattern=...) -> list:
        """
        避免使用keys函数
        :param pattern:
        :return:
        """
        warnings.warn(
            "This module is deprecated.", DeprecationWarning, stacklevel=2
        )
        return super().keys(pattern)
