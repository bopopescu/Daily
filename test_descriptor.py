#!/usr/bin/env python
# -*- coding: utf-8 -*-
from functools import wraps
'''
描述符 一个类如果实现了__get__,__set__,__del__方法(三个方法不一定要全部都实现)，并且该类的实例对象通常是另一个类的类属性，那么这个类就是一个描述符
__get__,__set__,__del__的具体声明如下：

      __get__(self, instance, owner)
      __set__(self, instance, value)
      __delete__(self, instance)

其中：
      __get__ 用于访问属性。它返回属性的值，或者在所请求的属性不存在的情况下出现 AttributeError 异常。
      __set__ 将在属性分配操作中调用。不会返回任何内容。
      __delete__ 控制删除操作。不会返回内容。

     只实现__get__方法的对象是非数据描述符，意味着在初始化之后它们只能被读取。而同时实现__get__和__set__的对象是数据描述符，意味着这种属性是可读写的。

     Python中必须添加额外的类型检查逻辑代码才能做到这一点，这就是描述符的初衷
'''
class name_des(object):

    def __init__(self):
        self._name = None

    def __get__(self, instance, owner):
        print("call __get__")
        return self._name

    def __set__(self, instance, value):
        print("call __set__")
        if isinstance(value, str):
            self._name = value
        else:
            raise  TypeError("Must be an string")


class Test(object):
    name = name_des()



def log(header, footer):
    def log_to_func(fun):
        def return_fun(*args, **kwargs):
            print(header)
            fun(*args, **kwargs)
            print(footer)
        return return_fun
    return log_to_func()


'''
在使用 Decorator 的过程中，难免会损失一些原本的功能信息。 
functools.wraps 则可以将原函数对象的指定属性复制给包装函数对象, 默认有 __module__、__name__、__doc__,或者通过参数选择'''
def logged(func):
    def with_logging(*args, **kwargs):
        print(func.__name__ + " was called")
        return func(*args, **kwargs)
    return with_logging

def logged_n(func):
    @wraps(func)
    def with_logging(*args, **kwargs):
        print(func.__name__ + " was called")
        return func(*args, **kwargs)
    return with_logging


# @logged
# def f(x):
#     return x + x*x

@logged
def f(x):
    "aaa"
    return x + x*x
# def f(x):
#     return x + x*x

# f = logged(f)
print(f.__name__)
print(f.__doc__)

'''类装饰器'''
def drinkable(message):
    def drinkable_to_return(cls):
        def drink(self):
            print("I can drink" + message)
        cls.drink = drink
        return cls
    return drinkable_to_return

@drinkable("water")
class test(object):
    pass

t = test()
t.drink()