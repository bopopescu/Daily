#!/usr/bin/env python
# -*- coding: utf-8 -*-
import gevent.monkey
gevent.monkey.patch_socket()
import time
import random
import gevent
import requests
from functools import wraps

def fetch(pid):
    response = requests.get('http://192.168.1.2:29600/api/v1/wx/app/get_dabaima')
    json_result = response.json()
    datetime = json_result['code']

    # print('Process %s: %s' % (pid, datetime))
    return json_result['code']

def task(pid):
    """
    Some non-deterministic task
    """
    gevent.sleep(random.randint(0,2)*0.001)
    print('Task %s done' % pid)

def time_logged(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        res = func(*args, **kwargs)
        end = time.time()
        print("spend time: %s" %(end-start))
        return res
    return wrapper


@time_logged
def synchronous():
    for i in range(1,100):
        # fetch(i)
        task(i)

@time_logged
def asynchronous():
    threads = []
    for i in range(1,100):
        # threads.append(gevent.spawn(fetch, i))
        threads.append(gevent.spawn(task, i))
    gevent.joinall(threads)


print('Asynchronous:')
asynchronous()

print('Synchronous:')
synchronous()



def f(url):
    print('GET: %s' % url)
    resp = requests.get(url, verify=True)
    data = resp.text
    print('%d bytes received from %s.' % (len(data), url))

gevent.joinall([
        gevent.spawn(f, 'http://python.jobbole.com/87041//'),
        gevent.spawn(f, 'http://python.jobbole.com'),
        gevent.spawn(f, 'http://blog.jobbole.com/113819/'),
])