#!/usr/bin/env python
# -*- coding: utf-8 -*-
from functools import wraps
import time
import gc

def time_it(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        st = time.time()
        res = func(*args, **kwargs)
        print("Time cost {duration}".format(duration=time.time() - st))
        return res
    return wrapper

@time_it
def test_no_gc():
    data = range(1, 50000000)
    wdict = dict(zip(data, data))

@time_it
def test_gc():
    gc.disable()
    data = range(1, 50000000)
    wdict = dict(zip(data, data))
    gc.enable()



test_no_gc()    #0.8122742176055908
test_gc()

