#!/usr/bin/env python
# -*- coding: utf-8 -*-
import datetime
import calendar

def increase_year(year, source=None):
    '''
    根据给定的source date, 获得增加一定天数的时间
    '''
    source = source if source else datetime.datetime.now()
    _y = source.year + year
    _m = source.month
    _d = min(source.day, calendar.monthrange(_y, source.month)[1])
    return type(source)(_y, _m, _d)
