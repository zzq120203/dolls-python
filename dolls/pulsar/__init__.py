# !/usr/bin/env Python3
# -*- coding: utf-8 -*-
# @Author   : zhangzhanqi
# @FILE     : __init__.py.py
# @Time     : 2022/6/16 10:52
import pulsar
from .client import PProducer, PConsumer


class Pulsar(object):

    def __init__(self, url: str, auth=None, issl: bool = False) -> None:
        if issl:
            if not url.startswith("pulsar+ssl://"):
                url = "pulsar+ssl://" + url
            if url.startswith("pulsar://"):
                raise Exception("issl not ssl?")
        else:
            if not url.startswith("pulsar://"):
                url = "pulsar://" + url
        self.__client = pulsar.Client(service_url=url, authentication=auth)

    def producer(self, topic: str, schema=pulsar.schema.StringSchema(), max_message=1024 * 1024, **kwargs) -> PProducer:
        return PProducer(client=self.__client, topic=topic, schema=schema, max_message=max_message, **kwargs)

    def consumer(self, topic: str, sub_name: str, schema=pulsar.schema.StringSchema(), **kwargs) -> PConsumer:
        return PConsumer(client=self.__client, topic=topic, sub_name=sub_name, schema=schema, **kwargs)

    def close(self):
        self.__client.close()
