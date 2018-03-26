#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
一个程序专门往列表中添加数字，另一个程序专门提取数字进行处理，二者共同维护这样一个列表
'''
import time
import threading
import random

class Producer(threading.Thread):
    '''产生随机数，并将其加入整数列表'''
    def __init__(self, lock, integer_list):
        threading.Thread.__init__(self)
        self.lock = lock
        self.integer_list = integer_list

    def run(self):
        while True:  #一直尝试获得锁来添加整数
            random_integer = random.randint(1,100)
            with self.lock:
                self.integer_list.append(random_integer)
                print("integer_list add integer {}".format(random_integer))
            time.sleep(1.2 * random.random())  #sleep随机时间，通过乘1.2来减慢生产的速度

class Comsumer(threading.Thread):
    def __init__(self, lock, integer_list):
        threading.Thread.__init__(self)
        self.lock = lock
        self.integer_list = integer_list

    def run(self):
        while True:
            with self.lock:
                if self.integer_list:
                    integer = self.integer_list.pop()
                    print("integer_list lose integer {}".format(integer))
                    time.sleep(random.random())
                else:
                    print("there is no integer in the list")

def main():
    integer_list = []
    lock = threading.Lock()
    th1 = Producer(lock, integer_list)
    th2 = Comsumer(lock, integer_list)
    th1.start()
    th2.start()

if __name__ == "__main__":
    main()

'''
我们可以看到，整数每次产生都会被迅速消耗掉，消费者没有东西可以处理，但是依然不停地询问
是否有东西可以处理（while True），这样不断地询问会比较浪费CPU等资源（特别是询问之后不只是print而是加入计算等）'''