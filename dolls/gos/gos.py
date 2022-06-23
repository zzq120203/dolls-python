import uuid
from typing import Any, Union

import requests
from urllib.parse import urlencode


class Gos(object):
    def __init__(
            self,
            url: str,
            user: str,
            password: str,
            namespace: str = None,
            version: int = 1,
            timeout: int = 10
    ) -> None:
        """

        :param url: gos 地址 ip:port
        :param user:
        :param password:
        :param namespace: 默认namespace
        :param version: 目前version:1
        :param timeout:
        """
        if url.startswith("http") or url.startswith("https"):
            self.url = url
        else:
            self.url = f"http://{url}"
        self.user = user
        self.password = password
        self.version = version
        self.timeout = timeout
        self.token = None
        self.namespace = namespace

        self.__login()

    def __login(self) -> None:
        """
        gos login return token
        POST http://[ip]:[port]/api/v1/login
        :return: stats, token
        """
        result = requests.post(
            url=f"{self.url}/api/v{self.version}/login",
            json={"user": self.user, "password": self.password},
            timeout=self.timeout
        ).json()

        stats = result.get("state", 1)
        if not stats == 0:
            raise BaseException(result.get("info", "unknown"))

        self.token = result.get("token", None)

    def logout(self) -> None:
        pass

    def put(
            self,
            data: bytes,
            namespace: str = None,
            key: str = None,
            content_type: Union[str,] = "image/jpeg",
            prop: Any = None
    ) -> Union[str, None]:
        """
        POST http://[ip]:[port]/api/v1/put
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
        headers = {
            "Content-Type": content_type
        }
        result = requests.post(
            url=f"{self.url}/api/v{self.version}/put?{urlencode(params)}",
            data=data,
            headers=headers,
            timeout=self.timeout
        )

        stats = result.status_code
        if stats == 401:
            self.__login()
            return self.put(data, namespace, key, content_type, prop)
        elif not stats == 200:
            raise BaseException(result.text)
        return key

    def get(
            self,
            key: str,
            namespace: str = None,
            intent: int = 11,
            is_url: bool = False
    ) -> Union[bytes, str, None]:
        """
        GET http://[ip]:[port]/api/v1/get
        :param is_url: 
        :param namespace: namespace
        :param key: 对象的key
        :param intent:
            0: 获取key对应的对象所有的元信息
            1: 获取key对应的对象的修改时间
            2: 获取key对应的对象的属性信息prop
            3: 获取key对应的对象的长度信息
            # 4: 获取key对应的对象的key
            # 9: 随机获取一个key
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
            "intent": intent
        }
        if key is not None:
            params["key"] = key
        url = f"{self.url}/api/v{self.version}/get?{urlencode(params)}"
        if is_url:
            return url

        result = requests.get(
            url=url,
            timeout=self.timeout
        )

        stats = result.status_code
        if stats == 401:
            self.__login()
            return self.get(key, namespace, intent, is_url)
        elif not stats == 200:
            raise BaseException(result.text)

        return result.content
