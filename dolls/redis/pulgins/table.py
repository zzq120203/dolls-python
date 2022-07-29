from typing import List, TypeVar

from redis import ResponseError
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
                primary_key = self.__redis_search.hget(f'{self.__table_name}_info', "pk")
                if not primary_key:
                    raise ValueError("table is not exist")
                self.__primary_key = primary_key
            else:
                self.__redis_search.hset(f'{self.__table_name}_info', "pk", primary_key)
                self.__primary_key = primary_key
        return self.__primary_key

    def create(self, fields: List[RTField]):
        """
        创建表
        :return:
        """
        try:
            pk = [field for field in fields if field.primary_key]
            if len(pk) == 0:
                pk = "doc"
            else:
                pk = pk[0].name
            self.__get_primary_key(pk)
            definition = IndexDefinition(prefix=[f'{self.__table_name}:', 'article:'], language="chinese")

            fields = [field.to_search_field() for field in fields]
            self.__redis_search.create_index(fields, definition=definition)
        except ResponseError as err:
            if str(err).startswith("Index"):
                raise ValueError("table already exists")
            else:
                raise err

    def desc(self):
        """
        表结构信息
        :return:
        """
        try:
            return self.__redis_search.desc()
        except ResponseError as err:
            if str(err) == "Unknown Index name":
                return None
            else:
                raise err

    def insert(self, mapping):
        """

        :param mapping:
        :return:
        """
        primary_key = self.__redis_search.hincrby(f'{self.__table_name}_info', self.__get_primary_key())
        mapping.update({self.__get_primary_key(): primary_key})
        self.__redis_search.hset(f'{self.__table_name}:{primary_key}', mapping=mapping)
        return primary_key

    def update(self, primary_key, mapping):
        """

        :param mapping:
        :param primary_key:
        :return:
        """
        mapping.update({self.__get_primary_key(): primary_key})
        self.__redis_search.hset(f'{self.__table_name}:{primary_key}', mapping=mapping)

    def search(self, query):
        """
        https://oss.redislabs.com/redisearch/Query_Syntax/
        :return:
        """
        try:
            return self.__redis_search.search(query)
        except ResponseError as err:
            if str(err).endswith("index"):
                return None
            else:
                raise err

    def delete(self, primary_key=None):
        """
        删除数据
        :param primary_key: 主键值
        :return:
        """
        try:
            if primary_key:
                self.__redis_search.delete(f'{self.__table_name}:{primary_key}')
                return 1
            return 0
        except ResponseError as err:
            if str(err) == "Unknown Index name":
                return 0
            else:
                raise err

    def drop(self):
        """
        删除表
        :return:
        """
        try:
            self.__redis_search.delete(f'{self.__table_name}_info')
            self.__redis_search.drop_index()
            return 1
        except ResponseError as err:
            if str(err) == "Unknown Index name":
                return 0
            else:
                raise err


