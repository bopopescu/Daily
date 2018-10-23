#!/usr/bin/env python
# -*- coding: utf-8 -*-
import redis
import time

# pool = redis.ConnectionPool(host="39.106.220.85", port=6379, decode_responses=True)
# r = redis.Redis(connection_pool=pool)
#
# # r.setex("fruit", "apple", 5)
# # print(r.get("fruit"))
# # time.sleep(5)
# # print(r.get("fruit"))
# r.hset("hash1", "a", 1)
# r.hset("hash1", "b", 2)
# r.hset("hash1", "c", 3)
# print(r.hgetall("hash1"))
# r.hdel("hash1", "b")
# print(r.hgetall("hash1"))


REDIS_HOST = "39.106.220.85"
REDIS_PORT = 6379
REDIS_DBID = 0


def operator_status(func):
    """get operation status"""
    def gen_status(*args, **kwargs):
        error, result = None, None
        try:
            result = func(*args, **kwargs)
        except Exception as e:
            error = str(e)
        return {'result': result, 'error': error}
    return gen_status


class MyRedis(object):
    """redis封装类 使用连接池"""

    def __init__(self):
        if not hasattr(MyRedis, "pool"):
            MyRedis.create_pool()
        self._connection = redis.Redis(connection_pool=MyRedis.pool)

    @staticmethod
    def create_pool():
        MyRedis.pool = redis.ConnectionPool(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    def str_set(self, key, value):
        return self._connection.set(key, value)

    def str_get(self, key):
        return self._connection.get(key)

    def str_del(self, key):
        return self._connection.delete(key)

    def hash_set(self, name, key, value):
        return self._connection.hset(name, key, value)

    def hash_get(self, name, key):
        return self._connection.hget(name, key)

    def hash_del(self, name, key):
        return self._connection.hdel(name, key)

    def hash_getall(self, name):
        return self._connection.hgetall(name)


if __name__ == '__main__':
    print(MyRedis().hash_getall("hash1"))
    print(MyRedis().hash_set("hash1", "b", "2"))
    print(MyRedis().hash_get("h1", "a"))
    print(MyRedis().hash_get("hash1", "b"))
    print(MyRedis().hash_del("hash1", "b"))
    print(MyRedis().hash_get("hash1", "b"))
