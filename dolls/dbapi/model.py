# !/usr/bin/env Python3
# -*- coding: utf-8 -*-
# @Author   : zhangzhanqi
# @FILE     : table_model.py
# @Time     : 2022/6/16 17:09
import re

import sqlalchemy


def get_field_type(type_name: str):
    type_name = type_name.lower()
    if type_name in ['varchar', 'str', 'string']:
        return sqlalchemy.String
    elif re.findall('^varchar\(\d+\)$', type_name) \
            or re.findall('^str\(\d+\)$', type_name) \
            or re.findall('^string\(\d+\)$', type_name):
        length = int(type_name[8:-1])
        return sqlalchemy.String(length)

    elif type_name in ['int', 'integer']:
        return sqlalchemy.Integer
    elif type_name in ['smallint', 'tinyint']:
        return sqlalchemy.SmallInteger
    elif type_name == 'bigint':
        return sqlalchemy.BigInteger
    elif type_name == 'float':
        return sqlalchemy.Float
    elif type_name == 'datetime':
        return sqlalchemy.DateTime
    elif type_name == 'text':
        return sqlalchemy.TEXT
    elif re.findall('^text\(\d+\)$', type_name):
        length = int(type_name[8:-1])
        return sqlalchemy.TEXT(length)
    elif type_name == 'json':
        return sqlalchemy.JSON
    else:
        return type_name
