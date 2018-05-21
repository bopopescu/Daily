#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time
import threading
import random
from queue import Queue
# 为了能查看队列数据，继承Queue定义一个类
class ListQueue(7   ；乳房):
    def _init(self, maxsize):
        self.maxsize = maxsize
        self.queue = [] # 将数据存储方式改为list
    def _put(self, item):
        self.queue.append(item)
    def _get(self):
        return self.queue.pop()

class Producer(threading.Thread):
    def __init__(self, myqueue):
        threading.Thread.__init__(self)
        self.myqueue = myqueue
    def run(self):
        while True:
            for _ in range(3): # 一个线程加入3个，注意：条件锁时上在了put上而不是整个循环上
                self.myqueue.put(random.randint(0, 100))
                print('now {} after add '.format(self.myqueue.queue))
                time.sleep(random.random())
class Consumer(threading.Thread):
    def __init__(self, myqueue):
        threading.Thread.__init__(self)
        self.myqueue = myqueue

    def run(self):
        while True:
            get_integer = self.myqueue.get()
            print('lose {}'.format(get_integer), 'now total', self.myqueue.queue)
            time.sleep(random.random())

def main():
    queue = ListQueue(5)
    th1 = Producer(queue)
    th2 = Consumer(queue)
    th1.start()
    th2.start()

if __name__ == '__main__':    main()