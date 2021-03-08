from typing import List, TypeVar

import six
from redisearch import IndexDefinition, TextField, NumericField, Query

RTQuery = Query
T = TypeVar('T')


class RTField(object):

    def __init__(self, name, type, primary_key=False, sortable=False, no_index=False,
                 no_stem=False, weight=1.0, phonetic_matcher=None):
        self.name = name
        self.type = type
        self.primary_key = primary_key
        self.weight = weight
        self.sortable = sortable
        self.no_stem = no_stem
        self.no_index = no_index
        self.phonetic_matcher = phonetic_matcher

    def to_search_field(self):
        if self.type == 'int':
            kwargs = {
                "name": self.name,
                "sortable": self.sortable,
                "no_index": self.no_index
            }
            return NumericField(**kwargs)
        if self.type == 'str' or self.type == 'map':
            kwargs = {
                "name": self.name,
                "weight": self.weight,
                "sortable": self.sortable,
                "no_stem": self.no_stem,
                "no_index": self.no_index,
                "phonetic_matcher": self.phonetic_matcher
            }
            return TextField(**kwargs)
        else:
            kwargs = {
                "name": self.name,
                "weight": self.weight,
                "sortable": self.sortable,
                "no_stem": self.no_stem,
                "no_index": self.no_index,
                "phonetic_matcher": self.phonetic_matcher
            }
            return TextField(**kwargs)


class Table(object):
    def __init__(self, redis_search, table_name):
        """
        :param redis_search:
        :param table_name:
        """
        self.__redis_search = redis_search
        self.__table_name = table_name

        self.__primary_key = None

    def __get_primary_key(self, primary_key=None):
        if not self.__primary_key:
            if not primary_key:
                primary_key = self.__redis_search.hget('{}_info'.format(self.__table_name), "pk")
                if not primary_key:
                    assert ValueError("table is not exist")
                self.__primary_key = primary_key
            else:
                self.__redis_search.hset('{}_info'.format(self.__table_name), "pk", primary_key)
                self.__primary_key = primary_key
        return self.__primary_key

    def create(self, fields: List[RTField]):
        """
        创建表
        :return:
        """
        pk = [field for field in fields if field.primary_key]
        if len(pk) == 0:
            pk = "doc"
        else:
            pk = pk[0].name
        self.__get_primary_key(pk)
        definition = IndexDefinition(prefix=['{}:'.format(self.__get_primary_key()), 'article:'])

        fields = [field.to_search_field() for field in fields if not field.primary_key]
        for f in fields:
            print(f.redis_args())
        self.__redis_search.create_index(fields, definition=definition)

    def desc(self):
        """
        表结构信息
        :return:
        """
        res = self.__redis_search.execute_command('FT.INFO', self.__table_name)
        it = six.moves.map(_to_string, res)
        return dict(six.moves.zip(it, it))

    def insert(self, mapping):
        """

        :param mapping:
        :return:
        """
        primary_key = self.__redis_search.hincrby('{}_info'.format(self.__table_name), self.__get_primary_key())
        self.__redis_search.hset('doc:{}'.format(primary_key), mapping=mapping)

    def update(self, primary_key, mapping):
        """

        :param mapping:
        :param primary_key:
        :return:
        """
        self.__redis_search.hset('doc:{}'.format(primary_key), mapping=mapping)

    def search(self, query):
        """
        https://oss.redislabs.com/redisearch/Query_Syntax/
        :return:
        """
        return self.__redis_search.search(query)

    def drop(self):
        """

        :return:
        """
        self.__redis_search.delete('{}_incr.{}'.format(self.__table_name, self.__get_primary_key()))
        self.__redis_search.drop_index()


def _to_string(s):
    if isinstance(s, six.string_types):
        return s
    elif isinstance(s, six.binary_type):
        return s.decode('utf-8', 'ignore')
    else:
        return s  # Not a string we care about
