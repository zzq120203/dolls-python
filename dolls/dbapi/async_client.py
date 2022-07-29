# !/usr/bin/env Python3
# -*- coding: utf-8 -*-
# @Author   : zhangzhanqi
# @FILE     : async_client.py
# @Time     : 2022/7/22 14:56
import sqlalchemy
from sqlalchemy.ext.asyncio import create_async_engine

from .dbcontext import DBContext, TableContext
from .model import get_field_type


class Client(object):
    def __init__(self, context: DBContext = None, **kwargs):
        """

        :param context:
        :param kwargs:
            scheme: str = 'mysql'
            host: str = 'localhost'
            port: str = '5432'
            user: str = 'root'
            password: str = '123456'
            db: str = 'test'
        """
        if context is not None:
            self.context = context
        else:
            self.context = DBContext(**kwargs)

        self.engine = create_async_engine(self.context.url, echo=False)

        self.metadata = sqlalchemy.MetaData(self.engine)

        self.metadata.reflect(bind=self.engine)

    async def close(self):
        await self.engine.dispose()

    def table_names(self):
        return self.metadata.tables.keys()

    def table(self, table_name, fields=None) -> TableContext:
        if fields is None:
            return self.metadata.tables.get(table_name)

        columns = []
        for field in fields:
            field["type_"] = get_field_type(field.get('type'))
            del field["type"]
            column = sqlalchemy.Column(**field)
            columns.append(column)

        return TableContext(table_name, self.metadata, *columns).create()


