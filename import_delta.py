#!/usr/bin/env python
# -*- coding: utf-8 -*-
from pymongo import MongoClient
import logging.handlers
import time
import pymongo

log_file = 'delta.log'
handler = logging.handlers.RotatingFileHandler(
    log_file, maxBytes=5 * 1024 * 1024, backupCount=5)  # 实例化handler
fmt = '%(asctime)s - %(filename)s:%(lineno)s - %(name)s - %(message)s'
formatter = logging.Formatter(fmt)  # 实例化formatter
handler.setFormatter(formatter)  # 为handler添加formatter
logger = logging.getLogger('a')  # 获取名为tst的logger
logger.addHandler(handler)  # 为logger添加handler
logger.setLevel(logging.DEBUG)


if __name__ == "__main__":
    master_client = MongoClient("jp.imcs.powzamedia.com", 27017)
    db = master_client.get_database("log_db")
    file = open("freeze_rate08-09.txt", "a")
    ip_list = [
        '164.52.0.184',
        '164.52.0.185',
        '164.52.0.186',
        '164.52.6.19',
        '164.52.6.20'
    ]

    # ip_list = [
    #     '164.52.0.180',
    #     '164.52.0.181'
    # ]
    for ip in ip_list:
        print(ip)
        # file.write("节点ip: " + ip + "\n")
        # pipeline = [{"$unwind": "$node_statistics"}, {"$unwind": "$node_statistics.node"},
        #              {"$match": {"time": {"$gte": 1533657600, "$lt": 1533744000}, "node_statistics.node.ip": ip}},
        #             {"$sort": {"time": 1}}]
        # results = db.statistic_node.aggregate(pipeline)
        query = {"start": {"$gte": 1533657600, "$lt": 1533744000}, "s_ip": ip}
        results = db.ori_node.find(query).sort([("start", pymongo.ASCENDING)])
        num = 0
        sum = 0
        for result in results:
            num += 1
            print(result)
            # logger.info("start statistics Ip: %s No.%s", ip, num)
            # freeze_rate = result["node_statistics"]["freeze_rate"]
            s_time = result.get("time", 0)
            print(s_time)
            # sum += freeze_rate
            # file.write(str(s_time) + " " + str(freeze_rate) + "\n")
        # average = round(sum / num, 2)
        # file.write("total:" + str(sum) + " num:" + str(num) + " average:" + str(average) + "\n")
        # print(average)
        # file.write("\n")
    master_client.close()
