from dolls.pydis import RedisPool

if __name__ == '__main__':
    pool = RedisPool(["192.168.101.66", 16379])

    xxxx = pool.search("xxxx")

    xxxx.hset("xxxx", "1111", "121312")

    print(xxxx.hget("xxxx", "1111"))