#!/usr/bin/env python
# -*- coding: utf-8 -*-
from hashlib import sha256
import hashlib
# x = 5
# y = 0  # y未知
# while sha256(f'{x*y}'.encode()).hexdigest()[:5] != "00000":
#     y += 1
# print(f'The solution is y = {y}')
md5 = hashlib.md5()
md5.update("nidadqdq".encode("utf-8"))
s2 = sha256("nidadqdq".encode("utf-8")).hexdigest()
print(md5.hexdigest())
print(s2)
str1 = "c8199b8f6144133ed0f8824402d2239dcae63085fa1845d377955062fcace2b1"
print(len(str1))