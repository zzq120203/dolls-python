from urllib.parse import urlparse

from .pool import RedisPool, RedisMode


def from_url(url, **kwargs) -> RedisPool:
    """

    :param url: redis[+cluster]://user:passwd@localhost:6379;localhost:6378/db
    :return: RedisPool
    """
    parsed = urlparse(url)
    up, urls = parsed.netloc.split("@")
    user, password = up.split(":")
    db = int(parsed.path[1:])

    if parsed.scheme not in ["redis", "redis+sentinel", "redis+cluster"]:
        raise Exception(f"schema({parsed.scheme}) not in schema [redis, redis+sentinel, redis+cluster]")
    if parsed.scheme == "redis":
        mode = 0
    elif parsed.scheme == "redis+sentinel":
        mode = 1
    elif parsed.scheme == "redis+cluster":
        mode = 2
    else:
        mode = 0

    return RedisPool(urls=urls, username=user, password=password, redis_mode=mode, db=db, **kwargs)
