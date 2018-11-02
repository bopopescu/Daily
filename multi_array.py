#!/usr/bin/env python
# -*- coding: utf-8 -*-
from multiprocessing import Process
from multiprocessing import Array

def func(i, ar):
    ar[i] = i
    for item in ar:
        print(item)
    print("------")

ar = Array('i', 6)
for i in range(6):
    ar[i] = i
ar[6] = 0

print(ar)
count = 0
for i in ar:
    count += i
print(count)

# for i in range(5):
#     p = Process(target=func, args=(i, ar,))
#     p.start()
# p.join()