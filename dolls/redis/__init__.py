from urllib.parse import urlparse

from .pool import RedisPool, RedisMode

__REDIS_SCHEME: set = {"redis", "redis+sentinel", "rediss", "redis+cluster", "redisc"}


def from_url(url: str, **kwargs) -> RedisPool:
    """

    :param url:
        standalone redis://[user]:passwd@localhost:6379[/db]
        sentinel redis+sentinel://[user]:passwd@localhost:6379[/db]/master
            or rediss://[user]:passwd@localhost:6379[/db]/master
        cluster redis+cluster://[user]:passwd@localhost:6379[/db]
            or redisc://[user]:passwd@localhost:6379[/db]
    :return: RedisPool
    """
    parsed = urlparse(url)
    user = parsed.username
    password = parsed.password
    urls = parsed.netloc
    if "@" in urls:
        urls = urls.split("@")[-1]
    master = None
    db = 0
    if parsed.path:
        path = parsed.path[1:]
        if "/" in path:
            db, master = path.split("/")
        else:
            try:
                db = int(path)
            except Exception:
                master = path

    if parsed.scheme not in __REDIS_SCHEME:
        raise Exception(f"schema({parsed.scheme}) not in scheme {__REDIS_SCHEME}")
    if parsed.scheme == "redis":
        mode = 0
    elif parsed.scheme in ["redis+sentinel", "rediss"]:
        mode = 1
        assert master is not None, f"master must not none on sentinel mode."
    elif parsed.scheme in ["redis+cluster", "redisc"]:
        mode = 2
    else:
        mode = 0

    if db:
        db = int(db)
    else:
        db = 0

    return RedisPool(urls=urls, username=user, password=password,
                     redis_mode=mode, db=db, master_name=master,
                     **kwargs)
