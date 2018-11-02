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


REDIS_HOST = ""
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

    def z_add(self, key, member, score):
        return self._connection.zadd(key, member, score)

    def z_rem(self, key, member):
        """删除制定元素，1表示成功，元素不存在返回0"""
        return self._connection.zrem(key, member)

    def z_range(self, key, score_min, score_max):
        """通过索引区间返回有序集合制定区域内的成员"""
        return self._connection.zrange(key, score_min, score_max)

    def z_card(self, key):
        """获取有序集合的成员数"""
        return self._connection.zcard(key)

    def z_rank(self, key, member):
        """获取有序集合中制定成员的索引"""
        return self._connection.zrank(key, member)

    def z_count(self, key, score_min, score_max):
        """计算有序集合中给定分数区间的成员数"""
        return self._connection.zcount(key, score_min, score_max)

    def z_remrangebyscore(self, key, score_min, score_max):
        """移除有序集合中给定分数区间的所有成员"""
        return self._connection.zremrangebyscore(key, score_min, score_max)

if __name__ == '__main__':
    r = MyRedis()
    print(r.hash_getall("hash1"))
    print(r.hash_set("hash1", "b", "2"))
    print(r.hash_get("h1", "a"))
    print(r.hash_get("hash1", "b"))
    print(r.hash_del("hash1", "b"))
    print(r.hash_get("hash1", "b"))
    print(r.z_add("push", "flu_id0", 1200))
    print(r.z_add("push", "flu_id1", 1201))
    print(r.z_range("push", 0, 2000))
    print(r.z_count("push", 0, 2000))
    print(r.z_rank("push", "flu_id10"))
    print(type("127.0.0.1"))
    print(r._connection.hset(str("127.0.0.1"), "b", 2))
