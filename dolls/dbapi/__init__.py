# !/usr/bin/env Python3
# -*- coding: utf-8 -*-
# @Author   : zhangzhanqi
# @FILE     : __init__.py.py
# @Time     : 2022/7/21 16:55
import sqlalchemy

from .client import Client as SQLClient
from .dbcontext import DBContext, SQLUrl, TableContext, SelectContext

if sqlalchemy.__version__.startswith("1.4"):
    from .async_client import Client as AsyncSQL


def from_url(url: str, sync=False):
    if sync:
        return AsyncSQL(url=url)
    return SQLClient(url=url)
