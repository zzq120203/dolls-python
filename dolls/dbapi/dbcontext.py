# !/usr/bin/env Python3
# -*- coding: utf-8 -*-
# @Author   : zhangzhanqi
# @FILE     : dbconfig.py
# @Time     : 2022/6/16 11:08
from typing import Optional, Dict, Any
from urllib.parse import urlparse

import sqlalchemy
from pydantic import BaseModel, validator, AnyUrl
from sqlalchemy.engine import CursorResult

POSTGRES_SCHEMES = {
    'postgres',
    'postgresql',
    'postgresql+asyncpg',
    'postgresql+pg8000',
    'postgresql+psycopg2',
    'postgresql+psycopg2cffi',
    'postgresql+py-postgresql',
    'postgresql+pygresql',
}

MYSQL_SCHEMES = {
    'mysql',
    'mysql+pymysql',
    'mysql+aiomysql',
}

ORACLE_SCHEMES = {
    'oracle',
    'oracle+cx_oracle',
}

DAMENG_SCHEMES = {
    'dm',
    'dm+dmpython'
}

ELASTICSEARCH_SCHEMES = {
    'elasticsearch',
    'elasticsearch+http',
    'odelasticsearch+https'
}


class SQLUrl(AnyUrl):
    allowed_schemes = POSTGRES_SCHEMES | MYSQL_SCHEMES | \
                      ORACLE_SCHEMES | DAMENG_SCHEMES | \
                      ELASTICSEARCH_SCHEMES
    host_required = False


class DBContext(BaseModel):
    scheme: Optional[str] = None
    host: Optional[str] = None
    port: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None
    db: Optional[str] = None

    url: Optional[SQLUrl] = None

    @validator("scheme", pre=True)
    def parse_scheme(cls, v: Optional[str], values: Dict[str, Any]) -> Any:
        if isinstance(v, str):
            return v.lower()
        url = values.get("url")
        p = urlparse(url)
        return p.scheme

    @validator("host", pre=True)
    def parse_host(cls, v: Optional[str], values: Dict[str, Any]) -> Any:
        if isinstance(v, str):
            return v
        url = values.get("url")
        p = urlparse(url)
        return p.hostname

    @validator("port", pre=True)
    def parse_port(cls, v: Optional[str], values: Dict[str, Any]) -> Any:
        if isinstance(v, str):
            return v
        url = values.get("url")
        p = urlparse(url)
        return p.port

    @validator("user", pre=True)
    def parse_user(cls, v: Optional[str], values: Dict[str, Any]) -> Any:
        if isinstance(v, str):
            return v
        url = values.get("url")
        p = urlparse(url)
        return p.username

    @validator("password", pre=True)
    def parse_pw(cls, v: Optional[str], values: Dict[str, Any]) -> Any:
        if isinstance(v, str):
            return v
        url = values.get("url")
        p = urlparse(url)
        return p.password

    @validator("db", pre=True)
    def parse_db(cls, v: Optional[str], values: Dict[str, Any]) -> Any:
        if isinstance(v, str):
            return v
        url = values.get("url")
        p = urlparse(url)
        return p.path[1:]

    @validator("url", pre=True)
    def assemble_db_connection(cls, v: Optional[str], values: Dict[str, Any]) -> Any:
        if isinstance(v, str):
            return v

        scheme = values.get('scheme', 'mysql')

        return SQLUrl.build(
            scheme=scheme,
            user=values.get("user"),
            password=values.get("password"),
            host=values.get("host"),
            port=values.get("port"),
            path=f"/{values.get('db', '')}",
        )

    def is_mysql(self) -> bool:
        return self.type in ['mysql']

    def is_postgresql(self):
        return self.type in ['pg', 'postgres', 'pgsql', 'postgresql']

    def is_elasticsearch(self):
        return self.type in ['elasticsearch', 'es']


class SelectContext(sqlalchemy.sql.Select):
    def where(self, *whereclause):
        return self

    def filter(self, *criteria):
        return self

    def join(self, target, onclause=None, isouter=False, full=False):
        return self

    def with_only_columns(self, *columns):
        return self

    def limit(self, limit):
        return self

    def offset(self, offset):
        return self

    def group_by(self, *clauses):
        return self

    def order_by(self, *clauses):
        return self

    def execute(self, *multiparams, **params) -> CursorResult:
        pass


class TableContext(sqlalchemy.Table):

    def select(self, whereclause=None, **kwargs) -> SelectContext:
        pass
