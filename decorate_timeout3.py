#!/usr/bin/env python
# -*- coding: utf-8 -*-
import threading
import signal, functools
from time import sleep, time
from multiprocessing.dummy import Pool as ThreadPool

class TimeoutError(Exception): pass


def timeout(seconds, error_message="Timeout Error: the cmd 30s have not finished."):
    def decorated(func):
        result = ""

        def _handle_timeout(signum, frame):
            global result
            result = error_message
            raise TimeoutError(error_message)

        def wrapper(*args, **kwargs):
            global result
            signal.signal(signal.SIGALRM, _handle_timeout)
            signal.alarm(seconds)

            try:
                result = func(*args, **kwargs)
            finally:
                signal.alarm(0)
                return result
            return result

        return functools.wraps(func)(wrapper)

    return decorated




@timeout(1)
def processNum(num):
    num_add = num + 1
    # results.append(str(threading.current_thread()) + ": " + str(num) + " → " + str(num_add))
    sleep(2)
    return str(threading.current_thread()) + ": " + str(num) + " → " + str(num_add)

def main():
    ts = time()
    pool = ThreadPool(4)
    results = pool.map(processNum, range(4))
    pool.close()
    pool.join()
    for _ in results:
        print(_)
    print("cost time is: {:.2f}s".format(time() - ts))


if __name__ == "__main__":
    main()