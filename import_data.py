#!/usr/bin/env python
# -*- coding: utf-8 -*-
from pymongo import MongoClient
import logging.handlers
import time

log_file = 'jp.log'
handler = logging.handlers.RotatingFileHandler(
    log_file, maxBytes=5 * 1024 * 1024, backupCount=5)  # 实例化handler
fmt = '%(asctime)s - %(filename)s:%(lineno)s - %(name)s - %(message)s'
formatter = logging.Formatter(fmt)  # 实例化formatter
handler.setFormatter(formatter)  # 为handler添加formatter
logger = logging.getLogger('a')  # 获取名为tst的logger
logger.addHandler(handler)  # 为logger添加handler
logger.setLevel(logging.DEBUG)

master_client = MongoClient("jp.imcs.powzamedia.com", 27017)
db = master_client.get_database("log_db")

if __name__ == "__main__":
    file = open("other.txt", "a")
    ip_list = [
        '164.52.6.20',
        '164.52.0.182',
        '164.52.0.184',
        '164.52.0.186',
        '164.52.0.178',
        '164.52.0.183',
        '164.52.0.185',
        '164.52.6.19'
    ]
    # ip_list = [
    #     '164.52.0.180',
    #     '164.52.0.181'
    # ]
    for ip in ip_list:
        file.write("节点ip: " + ip + "\n")
        pipeline = [{"$unwind": "$node_statistics"}, {"$unwind": "$node_statistics.node"},
                     {"$match": {"time": {"$gte": 1532880000, "$lt": 1532966400}, "node_statistics.node.ip": ip}},
                    {"$sort": {"time": 1}}]
        results = db.statistic_node.aggregate(pipeline)
        num = 0
        sum = 0
        for result in results:
            num += 1
            logger.info("start statistics Ip: %s No.%s", ip, num)
            node_statistics = result["node_statistics"]
            s_time = result["time"]
            freeze_rate = node_statistics.get("freeze_rate")
            # sum += freeze_rate
            file.write(str(s_time) + " " + str(freeze_rate) + "\n")
        # average = round(sum / num, 2)
        # file.write("total:" + str(sum) + " num:" + str(num) + " average:" + str(average) + "\n")
        # print(average)
        file.write("\n")

