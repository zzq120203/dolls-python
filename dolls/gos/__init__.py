# !/usr/bin/env Python3
# -*- coding: utf-8 -*-
# @Author   : zhangzhanqi
# @FILE     : __init__.py.py
# @Time     : 2022/6/16 10:51

from .aiogos import AsyncGos
from .client import Gos


def from_url(url: str, **kwargs) -> Gos:
    return Gos(url=url, **kwargs)
