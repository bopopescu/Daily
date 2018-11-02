#!/usr/bin/env python
# -*- coding: utf-8 -*-
import configparser

cp = configparser.ConfigParser()
cp.read('producer.conf', encoding="utf-8")


print(cp.get("setting", "server_ip"))
print(cp.get("setting", "server_ip2"))