#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
可以在第一次查询到列表为空的时候就开始等待，直到列表不为空（收到通知而不是一遍一遍地查询），资源开销就可以节省很多。
Condition对象就可以解决这个问题，它与一般锁的区别在于，除了可以acquire release，还多了两个方法wait notify
'''
import time
import threading
import random

class Producer(threading.Thread):
    '''产生随机数，并将其加入整数列表'''
    def __init__(self, condition, integer_list):
        threading.Thread.__init__(self)
        self.condition = condition
        self.integer_list = integer_list

    def run(self):
        while True:  #一直尝试获得锁来添加整数
            random_integer = random.randint(1,100)
            with self.condition:
                self.integer_list.append(random_integer)
                print("integer_list add integer {}".format(random_integer))
                self.condition.notify()
            time.sleep(1.2 * random.random())  #sleep随机时间，通过乘1.2来减慢生产的速度

class Comsumer(threading.Thread):
    def __init__(self, condition, integer_list):
        threading.Thread.__init__(self)
        self.condition = condition
        self.integer_list = integer_list

    def run(self):
        while True:
            with self.condition:
                if self.integer_list:
                    integer = self.integer_list.pop()
                    print("integer_list lose integer {}".format(integer))
                    time.sleep(random.random())
                else:
                    print("there is no integer in the list")
                    self.condition.wait()

def main():
    integer_list = []
    conditon = threading.Condition()
    th1 = Producer(conditon, integer_list)
    th2 = Comsumer(conditon, integer_list)
    th1.start()
    th2.start()

if __name__ == "__main__":
    main()
