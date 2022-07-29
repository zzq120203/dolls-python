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

