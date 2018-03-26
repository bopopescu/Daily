#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time
import threading
import random
from queue import Queue
class Producer(threading.Thread):
    def __init__(self, queue):
        threading.Thread.__init__(self)
        self.queue = queue

    def run(self):
        while True:
            random_integer = random.randint(0, 100)
            self.queue.put(random_integer)
            print('add {}'.format(random_integer))
            time.sleep(random.random())

class Consumer(threading.Thread):
    def __init__(self, queue):
        threading.Thread.__init__(self)
        self.queue = queue

    def run(self):
        while True:
            get_integer = self.queue.get()
            print('lose {}'.format(get_integer))
            time.sleep(random.random())

def main():
    queue = Queue()
    th1 = Producer(queue)
    th2 = Consumer(queue)
    th1.start()
    th2.start()

if __name__ == '__main__':
    main()