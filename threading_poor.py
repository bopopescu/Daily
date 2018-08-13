#!/usr/bin/env python
# -*- coding: utf-8 -*-

import threadpool
import time


def sayhello(a, b):
    print("hello: " + a + b)
    time.sleep(2)


def main():
    global result
    seed = [(["a", "1"], None), (["b", "2"], None), (["c", "3"], None)]
    start = time.time()
    task_pool = threadpool.ThreadPool(5)
    requests = threadpool.makeRequests(sayhello, seed)
    for req in requests:
        task_pool.putRequest(req)
    task_pool.wait()
    end = time.time()
    time_m = end - start
    print("time: " + str(time_m))
    start1 = time.time()
    for each_tuple in seed:
        sayhello(each_tuple[0][0], each_tuple[0][1])
    end1 = time.time()
    print("time1: " + str(end1 - start1))


if __name__ == '__main__':
    main()
