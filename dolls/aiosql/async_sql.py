# !/usr/bin/env Python3
# -*- coding: utf-8 -*-
# @Author   : zhangzhanqi
# @FILE     : async_sql.py
# @Time     : 2022/6/16 11:06

import sqlalchemy
from sqlalchemy.ext.asyncio import create_async_engine

from .db_config import SUPPORT_DB, DBConfig
from .table_model import TYPE_RELATION


class SQLAsync(object):
    def __init__(self, config: DBConfig):
        self.config = config
        if self.config.type not in SUPPORT_DB:
            raise Exception(f"db not in support db, "
                            f"db: {self.config.type}, support: {SUPPORT_DB}")

        self.engine = create_async_engine(self.config.url, echo=True)

        self.tables = {}

        self.metadata = sqlalchemy.MetaData(self.engine)

    async def close(self):
        await self.engine.dispose()

    def table(self, name, fields):
        if name in self.tables:
            return self.tables[name]
        columns = []
        for field in fields:
            column = sqlalchemy.Column(field.get('name'), TYPE_RELATION.get(field.get('type')),
                                       primary_key=field.get('primary_key', False), comment=field.get('comment', None))

            columns.append(column)

        table = sqlalchemy.Table(name, self.metadata, *columns)
        self.tables[name] = table
        return table
