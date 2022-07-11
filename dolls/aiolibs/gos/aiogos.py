# !/usr/bin/env Python3
# -*- coding: utf-8 -*-
# @Author   : zhangzhanqi
# @FILE     : aiogos.py
# @Time     : 2022/6/30 16:01
import datetime
import uuid
from typing import Any, Union, Final

import aiohttp
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from yarl import URL


class GosAsync(object):
    def __init__(
            self,
            url: str,
            version: str = "v1",
            **kwargs
    ) -> None:
        """

        :param url: gos://user:password@localhost:7000/namespace
        :param version: 目前version:v1
        """
        url = URL(url)
        assert url.scheme == "gos"
        self.user: Final[str] = url.user
        self.password: Final[str] = url.password
        self.namespace: Final[str] = url.path[1:]
        self.version: Final[str] = version

        self.base_url: Final[URL] = URL(f"http://{url.host}:{url.port}/api/{self.version}")

        self.token = None

        self.aps = AsyncIOScheduler(timezone='Asia/Shanghai')
        self.aps.add_job(self.__login, 'interval', seconds=5 * 60, next_run_time=datetime.datetime.now())
        self.aps.start()

    async def close(self):
        self.aps.shutdown()
        await self.__logout()

    async def __login(self) -> None:
        """
        gos login return token
        POST /api/v1/login
        :return: stats, token
        """
        url = self.base_url / "login"
        body = {"user": self.user, "password": self.password}
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=body) as response:
                result = await response.json()

                stats = result.get("state", 1)
                if not stats == 0:
                    raise BaseException(result.get("info", "unknown"))

                self.token = result.get("token", None)

    async def __logout(self) -> None:
        pass

    async def put(
            self,
            data: bytes,
            namespace: str = None,
            key: str = None,
            content_type: Union[str] = "image/jpeg",
            prop: Any = None
    ) -> Union[str, None]:
        """
        POST /api/v1/put
        :param namespace: namespace
        :param key: 对象的key
        :param content_type:
        :param data: 对象的数据内容
        :param prop: 对象的属性信息
        :return: stats, key
        """

        ns = namespace or self.namespace
        if ns is None:
            raise BaseException("namespace must not be empty")

        if key is None:
            key = str(uuid.uuid4())

        if self.token is None:
            raise BaseException("token must not be None.")

        params = {
            "token": self.token,
            "ns": ns,
            "key": key
        }
        url = self.base_url / "put" % params

        headers = {
            "Content-Type": content_type
        }

        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.post(url, data=data) as response:
                if response.status != 200:
                    raise BaseException(await response.text())
                return await response.json()

    async def get(
            self,
            key: str,
            namespace: str = None,
            intent: int = 11,
    ) -> Union[bytes, None]:
        """
        GET /api/v1/get
        :param is_url:
        :param namespace: namespace
        :param key: 对象的key
        :param intent:
            0: 获取key对应的对象所有的元信息
            1: 获取key对应的对象的修改时间
            2: 获取key对应的对象的属性信息prop
            3: 获取key对应的对象的长度信息
            10: 获取key对应的对象的数据内容
            11: 获取key对应的对象的数据内容，
                如果写入时制定了对象的类型，
                则将该类型增加到HTTP响应的头部
            12: 获取key对应的对象的数据内容区间，
                区间由用户指定，遵循HTTP头要求，
                形如"Range: bytes=0-1024"

        :return:
        """

        ns = namespace or self.namespace
        if ns is None:
            raise BaseException("namespace must not be empty")

        if self.token is None:
            raise BaseException("token must not be None.")

        params = {
            "token": self.token,
            "ns": ns,
            "intent": intent,
            "key": key
        }
        url = self.base_url / "get" % params
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status != 200:
                    raise BaseException(await response.text())
                return await response.read()

    def obj_uri(
            self,
            key: str,
            namespace: str = None,
            intent: int = 11,
    ) -> str:

        ns = namespace or self.namespace
        if ns is None:
            raise BaseException("namespace must not be empty")

        if self.token is None:
            raise BaseException("token must not be None.")

        params = {
            "token": self.token,
            "ns": ns,
            "intent": intent,
            "key": key
        }
        return str(self.base_url / "get" % params)
