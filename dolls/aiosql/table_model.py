# !/usr/bin/env Python3
# -*- coding: utf-8 -*-
# @Author   : zhangzhanqi
# @FILE     : table_model.py
# @Time     : 2022/6/16 17:09
import sqlalchemy

TYPE_RELATION = {
    'str': sqlalchemy.String,
    'varchar': sqlalchemy.String,
    'int': sqlalchemy.Integer,
    'integer': sqlalchemy.Integer,
    'smallint': sqlalchemy.SmallInteger,
    'tinyint': sqlalchemy.SmallInteger,
    'bigint': sqlalchemy.BigInteger,
    'text': sqlalchemy.TEXT
}


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
