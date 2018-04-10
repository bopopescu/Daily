#!/usr/bin/env python
# -*- coding: utf-8 -*-
from functools import wraps
import time
import gc
from guppy import hpy

def time_it(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        st = time.time()
        res = func(*args, **kwargs)
        print("Time cost {duration}".format(duration=time.time() - st))
        return res
    return wrapper


@time_it
def test_gc(way=1):
    for i in range(1, 5000000):
        if way == 1:
            pass
        else:
            del i
    if way == 1 or way == 2:
        pass
    else:
        gc.collect()

if __name__ == "__main__":
    print("Test way 1: just pass")
    test_gc(way=1)
    time.sleep(20)
    print("Test way 2: just del")
    test_gc(way=2)
    time.sleep(20)
    print("Test way 3: del, and then gc.collection()")
    test_gc(way=3)
    time.sleep(20)

