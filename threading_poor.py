#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# import threadpool
# import time
#
#
# def sayhello(a, b):
#     print("hello: " + a + b)
#     time.sleep(2)
#
#
# def main():
#     global result
#     seed = [(["a", "1"], None), (["b", "2"], None), (["c", "3"], None)]
#     start = time.time()
#     task_pool = threadpool.ThreadPool(5)
#     requests = threadpool.makeRequests(sayhello, seed)
#     for req in requests:
#         task_pool.putRequest(req)
#     task_pool.wait()
#     end = time.time()
#     time_m = end - start
#     print("time: " + str(time_m))
#     start1 = time.time()
#     for each_tuple in seed:
#         sayhello(each_tuple[0][0], each_tuple[0][1])
#     end1 = time.time()
#     print("time1: " + str(end1 - start1))
#
#
# if __name__ == '__main__':
#     main()

from multiprocessing import Pool
import os
import time
import random


def run_task(name):
    print('Parent process %s.' % os.getppid())
    print('Task %s (pid = %s) is running...' % (name, os.getpid()))
    time.sleep(random.random() * 3)
    print('Task %s end.' % name)


if __name__ == '__main__':
    print('Current process %s.' % os.getpid())
    p = Pool(processes=3)

    for i in range(5):
        p.apply_async(run_task, args=(i,))
    print('Waiting for all subprocesses done...')
    p.close()
    p.join()
    print('All subprocesses done.')
