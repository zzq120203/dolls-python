import six
from redis import Redis
from redisearch import Client

def to_string(s):
    if isinstance(s, six.string_types):
        return s
    elif isinstance(s, six.binary_type):
        return s.decode('utf-8', 'ignore')
    else:
        return s  # Not a string we care about


class Search(Redis, Client):
    def __init__(self, index_name, connection_pool):
        super(Search, self).__init__(connection_pool=connection_pool)
        Client.__init__(self, index_name, conn=self)

    def desc(self):
        """
        表结构信息
        :return:
        """
        res = self.execute_command('FT.INFO', self.index_name)
        it = six.moves.map(to_string, res)
        return dict(six.moves.zip(it, it))

    def table(self):
        from .table import Table
        return Table(self, self.index_name)

