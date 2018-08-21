#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
代码是由主进程里面的主线程从上到下执行的,
我们在主线程里面又创建了两个子进程，子进
程里面也是子线程在干活，这个子进程在主进
程里面
"""
import multiprocessing
import time
import threading


def func1(a1):
    time.sleep(3)
    print(a1)

li = []


def func2(i):
    li.append(i)
    print("你好", li)

if __name__ == "__main__":
    # t = multiprocessing.Process(target=func1, args=(12,))
    # t.start()
    # for i in range(10):
    #     p = multiprocessing.Process(target=func2, args=(i,))
    #     p.start()
    for i in range(10):
        p = threading.Thread(target=func2, args=(i,))
        p.start()
        p.join()
    print("end")
