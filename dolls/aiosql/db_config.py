# !/usr/bin/env Python3
# -*- coding: utf-8 -*-
# @Author   : zhangzhanqi
# @FILE     : db_config.py
# @Time     : 2022/6/16 11:08
from typing import Optional, Dict, Any

from pydantic import BaseModel, validator, AnyUrl

SUPPORT_DB = ['mysql',
              'pg', 'postgres', 'pgsql', 'postgresql',
              'oracle',
              ]


class DBConfig(BaseModel):
    type: str = 'mysql'
    host: str = 'localhost'
    port: str = '5432'
    user: str = 'root'
    password: str = '123456'
    db: str = 'test'

    url: Optional[AnyUrl] = None

    @validator("url", pre=True)
    def assemble_db_connection(cls, v: Optional[str], values: Dict[str, Any]) -> Any:
        if isinstance(v, str):
            return v

        schema = values.get('type', 'mysql')
        if schema == 'mysql':
            schema = 'mysql+pymysql'
        elif schema in ['pg', 'postgres', 'pgsql', 'postgresql']:
            schema = 'postgresql+asyncpg'

        return AnyUrl.build(
            scheme=schema,
            user=values.get("user"),
            password=values.get("password"),
            host=values.get("host"),
            port=values.get("port"),
            path=f"/{values.get('db', '')}",
        )
