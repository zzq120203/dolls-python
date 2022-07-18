# !/usr/bin/env Python3
# -*- coding: utf-8 -*-
# @Author   : zhangzhanqi
# @FILE     : table_model.py
# @Time     : 2022/6/16 17:09
import re

import sqlalchemy


def get_field_type(type_name):
    if type_name == 'varchar':
        return sqlalchemy.String
    elif re.findall('^varchar\(\d+\)$', type_name):
        length = int(type_name[8:-1])
        return sqlalchemy.String(length)

    elif type_name in ['int', 'integer']:
        return sqlalchemy.Integer
    elif type_name in ['smallint', 'tinyint']:
        return sqlalchemy.SmallInteger
    elif type_name == 'bigint':
        return sqlalchemy.BigInteger

    elif type_name == 'text':
        return sqlalchemy.TEXT
    elif re.findall('^text\(\d+\)$', type_name):
        length = int(type_name[5:-1])
        return sqlalchemy.TEXT(length)
    else:
        return None


class TableModel(sqlalchemy.Table):
    def __init__(self, name, metadata, *args, **kwargs):
        super().__init__(name, metadata, *args, **kwargs)

    async def async_create(self, if_not_exists=True):
        sql = sqlalchemy.schema.CreateTable(self, if_not_exists=if_not_exists)
        async with self.bind.begin() as session:
            await session.execute(sql)


class ModelMetaclass(type):
    """元类控制Model属性读写
    """

    def __getattr__(cls, item):
        if item.endswith('sa') and hasattr(cls, '__tables__'):
            return getattr(cls, '__tables__')[item]

    def __setattr__(self, key, value):
        raise Exception('class Model not allow to be set attr')


class Model(metaclass=ModelMetaclass):
    __tables__ = {}
    __metadata__ = sqlalchemy.MetaData()

    @classmethod
    def append(cls, table_sa: sqlalchemy.Table):
        cls.__tables__[str(table_sa.name) + '_sa'] = table_sa
