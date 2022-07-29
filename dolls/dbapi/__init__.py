# !/usr/bin/env Python3
# -*- coding: utf-8 -*-
# @Author   : zhangzhanqi
# @FILE     : __init__.py.py
# @Time     : 2022/7/21 16:55
from .dbcontext import DBContext
from .client import Client as SQLClient
from .async_client import Client as AsyncSQL


def from_url(url: str):
    return SQLClient(url=url)
