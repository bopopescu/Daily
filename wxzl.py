#!/usr/bin/env python
# -*- coding: utf-8 -*-
import re
from collections import Counter
sumsdata=[]


pattern = r',|\.|/|;|\'|`|\[|\]|<|>|\?|:|"|\{|\}|\~|!|@|#|\$|%|\^|&|\(|\)|-|=|\_|\+|，|。|、|；|‘|’|【|】|·|！| |…|（|）'
with open("日藏汉籍.txt",'r', encoding='utf-8') as fp:
    data = fp.readlines()
    fp.close()
for index, line in enumerate(data):
    l_item = re.split(pattern, line)
    for item in l_item:
        if item.find("藏本") > 0:
            print("第{}行  {}".format(index, item))
            sumsdata.append(item)

cnt = Counter(sumsdata)

print(cnt)