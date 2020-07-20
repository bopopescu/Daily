#!/usr/bin/env python
# -*- coding: utf-8 -*-
from pymongo import MongoClient
import time
import datetime
import logging
import logging.handlers
import configparser
from functools import wraps

mongodb_main_ip = ""
mongodb_main_port = ""
mongodb_subordinater_ip = ""
mongodb_subordinater_port = ""
mongodb_db = ""
mongodb_user = ""
mongodb_pwd = ""

link_data = {}
enc_data = {}
msc_data = {}

link_ipdata = {}
enc_ipdata = {}
msc_ipdata = {}

log_file = 'push_flow_statistics_hourly.log'
handler = logging.handlers.RotatingFileHandler(
    log_file, maxBytes=5 * 1024 * 1024, backupCount=5)  # 实例化handler
fmt = '%(asctime)s - %(filename)s:%(lineno)s - %(name)s - %(message)s'
formatter = logging.Formatter(fmt)  # 实例化formatter
handler.setFormatter(formatter)  # 为handler添加formatter
logger = logging.getLogger('push_flow_statistics_hourly')  # 获取名为tst的logger
logger.addHandler(handler)  # 为logger添加handler
logger.setLevel(logging.DEBUG)


def getconfig():
    global mongodb_main_ip
    global mongodb_main_port
    global mongodb_subordinater_ip
    global mongodb_subordinater_port
    global mongodb_db
    global mongodb_user
    global mongodb_pwd
    global start
    global abroad_link_direct_ips
    config = configparser.ConfigParser()
    config.read("pushflow.conf")
    mongodb_main_ip = config.get('setting', 'mongodb_main_ip')
    mongodb_main_port = config.getint('setting', 'mongodb_main_port')
    mongodb_subordinater_ip = config.get('setting', 'mongodb_subordinater_ip')
    mongodb_subordinater_port = config.getint('setting', 'mongodb_subordinater_port')
    mongodb_db = config.get('setting', 'mongodb_db')
    mongodb_user = config.get('setting', 'mongodb_user')
    mongodb_pwd = config.get('setting', 'mongodb_pwd')
    abroad_link_direct_ips = config.get("setting", "abroad_link_direct_ips")
    abroad_link_direct_ips = abroad_link_direct_ips.split(",")
    cur_time = int(time.time())
    start = int(time.mktime(time.strptime("2018-7-27 18:00", "%Y-%m-%d %H:%M")))

def timing(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        ts = time.time()
        res = func(*args, **kwargs)
        te = time.time()
        logger.info("'***********TIMING func:% args:[%r, %r]cost %2.4f sec", func.__name__, args, kwargs, te-ts)
        return res
    return wrapper

def get_database(
        mongodb_ip,
        mongodb_port,
        mongodb_user,
        mongodb_pwd,
        mongodb_db):
    # if mongodb_user == "":
    #     client = MongoClient(mongodb_ip, mongodb_port)
    # else:
    #     client = MongoClient(
    #         'mongodb://%s:%s@%s:%s/default_db?authSource=admin' %
    #         (mongodb_user, mongodb_pwd, mongodb_ip, mongodb_port))
    client = MongoClient(mongodb_ip, mongodb_port)
    return client.get_database(mongodb_db)


def handle_channels(ip, start_stamp, channels, d_ret):
    ''' 处理每条数据的channels '''
    for item in channels:
        channel_name = item.get("channel_name")
        l_channel = channel_name.split("/")
        if len(l_channel) != 3:
            continue
        channel_group = l_channel[0]
        channel_category = l_channel[1]
        channel_flowid = l_channel[2]
        channel_time = item.get("channeltime")
        if ip not in d_ret:
            d_ret[ip] = {}
        if channel_group not in d_ret[ip]:
            d_ret[ip][channel_group] = {}
        if channel_flowid not in d_ret[ip][channel_group]:
            d_ret[ip][channel_group][channel_flowid] = {
                "mlinkm": {},
                "mlinks": {}
            }
            if channel_category == "mlinkm":
                d_ret[ip][channel_group][channel_flowid]["mlinkm"]["end_channel_duration"] = channel_time
                d_ret[ip][channel_group][channel_flowid]["mlinkm"]["sum_channel_duration"] = 0
            elif "mlinks" in channel_category:
                d_ret[ip][channel_group][channel_flowid]["mlinks"][channel_category] = {
                    "start_timestamp": start_stamp * 1000,
                    "end_channel_duration": channel_time,
                    "sum_channel_duration": 0}
        else:
            if channel_category == "mlinkm":
                if not d_ret.get(ip).get(channel_group).get(
                        channel_flowid).get("mlinkm"):
                    d_ret[ip][channel_group][channel_flowid]["mlinkm"]["end_channel_duration"] = channel_time
                    d_ret[ip][channel_group][channel_flowid]["mlinkm"]["sum_channel_duration"] = 0
                else:
                    if channel_time >= d_ret[ip][channel_group][channel_flowid]["mlinkm"]["end_channel_duration"]:
                        d_ret[ip][channel_group][channel_flowid]["mlinkm"]["sum_channel_duration"] += (
                            channel_time - d_ret[ip][channel_group][channel_flowid]["mlinkm"]["end_channel_duration"])
                    else:
                        d_ret[ip][channel_group][channel_flowid]["mlinkm"]["sum_channel_duration"] += channel_time
                    d_ret[ip][channel_group][channel_flowid]["mlinkm"]["end_channel_duration"] = channel_time

            elif "mlinks" in channel_category:
                if not d_ret[ip][channel_group][channel_flowid].get(
                        "mlinks").get(channel_category):
                    d_ret[ip][channel_group][channel_flowid]["mlinks"][channel_category] = {
                        "start_timestamp": start_stamp * 1000,
                        "end_channel_duration": channel_time,
                        "sum_channel_duration": 0}
                else:
                    if channel_time >= d_ret[ip][channel_group][channel_flowid]["mlinks"][channel_category]["end_channel_duration"]:
                        d_ret[ip][channel_group][channel_flowid]["mlinks"][channel_category]["sum_channel_duration"] += (
                            channel_time - d_ret[ip][channel_group][channel_flowid]["mlinks"][channel_category]["end_channel_duration"])
                    else:
                        d_ret[ip][channel_group][channel_flowid]["mlinks"][channel_category]["sum_channel_duration"] += channel_time
                    d_ret[ip][channel_group][channel_flowid]["mlinks"][channel_category]["end_channel_duration"] = channel_time
    return True


def handle_enc_channels(start_stamp, channels):
    ''' 处理每条数据的channels '''
    for item in channels:
        channel_name = item.get("channel_name")
        if "EXT-ENC" not in channel_name:
            continue
        l_channel = channel_name.split("/")
        enc_id = l_channel[0]
        channel_group = l_channel[1]
        channel_category = l_channel[2]
        channel_flowid = l_channel[3]
        channel_time = item.get("channeltime")

        if channel_group not in enc_data:
            enc_data[channel_group] = {}
        if channel_flowid not in enc_data[channel_group]:
            enc_data[channel_group][channel_flowid] = {
                "mlinkm": {},
                "mlinks": {}
            }
            if channel_category == "mlinkm":
                if enc_id not in enc_data[channel_group][channel_flowid]["mlinkm"]:
                    enc_data[channel_group][channel_flowid]["mlinkm"][enc_id] = {}
                enc_data[channel_group][channel_flowid]["mlinkm"][enc_id]["end_channel_duration"] = channel_time
                enc_data[channel_group][channel_flowid]["mlinkm"][enc_id]["sum_channel_duration"] = 0
            elif "mlinks" in channel_category:
                if enc_id not in enc_data[channel_group][channel_flowid]["mlinks"]:
                    enc_data[channel_group][channel_flowid]["mlinks"][enc_id] = {}
                enc_data[channel_group][channel_flowid]["mlinks"][enc_id][channel_category] = {
                    "start_timestamp": start_stamp * 1000,
                    "end_channel_duration": channel_time,
                    "sum_channel_duration": 0}
        else:
            if channel_category == "mlinkm":
                if enc_id not in enc_data[channel_group][channel_flowid]["mlinkm"]:
                    enc_data[channel_group][channel_flowid]["mlinkm"][enc_id] = {}
                if not enc_data.get(channel_group).get(
                        channel_flowid).get("mlinkm").get(enc_id):
                    enc_data[channel_group][channel_flowid]["mlinkm"][enc_id]["end_channel_duration"] = channel_time
                    enc_data[channel_group][channel_flowid]["mlinkm"][enc_id]["sum_channel_duration"] = 0
                else:
                    if channel_time >= enc_data[channel_group][channel_flowid]["mlinkm"][enc_id]["end_channel_duration"]:
                        enc_data[channel_group][channel_flowid]["mlinkm"][enc_id]["sum_channel_duration"] += (
                            channel_time - enc_data[channel_group][channel_flowid]["mlinkm"][enc_id]["end_channel_duration"])
                    else:
                        enc_data[channel_group][channel_flowid]["mlinkm"][enc_id]["sum_channel_duration"] += channel_time
                    enc_data[channel_group][channel_flowid]["mlinkm"][enc_id]["end_channel_duration"] = channel_time

            elif "mlinks" in channel_category:
                if enc_id not in enc_data[channel_group][channel_flowid]["mlinks"]:
                    enc_data[channel_group][channel_flowid]["mlinks"][enc_id] = {}
                if not enc_data[channel_group][channel_flowid].get(
                        "mlinks").get(enc_id).get(channel_category):
                    enc_data[channel_group][channel_flowid]["mlinks"][enc_id][channel_category] = {
                        "start_timestamp": start_stamp * 1000,
                        "end_channel_duration": channel_time,
                        "sum_channel_duration": 0}
                else:
                    if channel_time >= enc_data[channel_group][channel_flowid][
                            "mlinks"][enc_id][channel_category]["end_channel_duration"]:
                        enc_data[channel_group][channel_flowid]["mlinks"][enc_id][channel_category]["sum_channel_duration"] += (
                            channel_time - enc_data[channel_group][channel_flowid]["mlinks"][enc_id][channel_category]["end_channel_duration"])
                    else:
                        enc_data[channel_group][channel_flowid]["mlinks"][enc_id][channel_category]["sum_channel_duration"] += channel_time
                    enc_data[channel_group][channel_flowid]["mlinks"][enc_id][channel_category]["end_channel_duration"] = channel_time
    return True


def get_link_ms_duration(d_channel, is_msc=False):
    ''' 获取link节点各频道组主播/辅播各自的推流时长 '''
    d_counter = {}
    for ip, ip_data in d_channel.items():
        d_duration = {}

        for channel_group, channel_data in ip_data.items():
            m_durations = 0
            s_durations = 0
            m_connect_durations = 0
            for flow_id, ms_data in channel_data.items():
                mlinkm = ms_data.get("mlinkm")
                mlinks = ms_data.get("mlinks")
                if mlinkm:
                    m_duration = mlinkm.get("sum_channel_duration")
                    if m_duration > 60 * 60 * 1000:
                        logger.info("[total]channel_group: %s, flow_id:%s, m_duration:%s", channel_group, flow_id,
                                    m_duration)
                        m_duration = 60 * 60 * 1000
                    m_durations += m_duration

                if mlinks:
                    s_duration, m_connect_duration = get_connect_duration(
                        list(mlinks.values()))
                    if m_connect_duration > 60 * 60 * 1000:
                        logger.info("[total]channel_group: %s, flow_id:%s, m_connect_duration:%s", channel_group,
                                    flow_id, m_connect_duration)
                        m_connect_duration = 60 * 60 * 1000
                    s_durations += s_duration
                    m_connect_durations += m_connect_duration

            if is_msc:
                d_duration[channel_group] = {
                    "msc_durations": m_connect_durations,
                    "connect_durations": m_connect_durations + s_durations
                }
            else:
                d_duration[channel_group] = {
                    "link_mdurations": m_durations,
                    "link_sdurations": s_durations
                }
        for channel_group, channel_data in d_duration.items():
            if channel_group not in d_counter:
                d_counter[channel_group] = channel_data
            else:
                if is_msc:
                    d_counter[channel_group]["msc_durations"] += channel_data["msc_durations"]
                    d_counter[channel_group]["connect_durations"] += channel_data["connect_durations"]
                else:
                    d_counter[channel_group]["link_mdurations"] += channel_data["link_mdurations"]
                    d_counter[channel_group]["link_sdurations"] += channel_data["link_sdurations"]
    return d_counter


def get_enc_duration(d_channel):
    ''' 获取ENC节点的推流总时长 '''
    d_duration = {}
    for channel_group, channel_data in d_channel.items():
        durations = 0
        for flow_id, ms_data in channel_data.items():
            mlinkm = ms_data.get("mlinkm")
            mlinks = ms_data.get("mlinks")
            if mlinkm:
                for enc_id, enc_id_data in mlinkm.items():
                    m_duration = enc_id_data.get("sum_channel_duration")
                    if m_duration > 60 * 60 * 1000:
                        logger.info("[ENC_TOTAL]channel_group: %s, flow_id:%s, enc_id:%s, m_duration:%s", channel_group, flow_id, enc_id,
                                    m_duration)
                        m_duration = 60 * 60 * 1000
                    durations += m_duration
            if mlinks:
                for enc_id, enc_id_data in mlinks.items():
                    for mlink, mlink_data in enc_id_data.items():
                        s_duration = mlink_data.get("sum_channel_duration")
                        if s_duration > 60 * 60 * 1000:
                            logger.info("[ENC_TOTAL]channel_group: %s, flow_id:%s, enc_id:%s, s_duration:%s",channel_group, flow_id, enc_id,
                                        mlink_data.get("sum_channel_duration"))
                            s_duration = 60 * 60 * 1000
                        durations += s_duration

        d_duration[channel_group] = {
            "enc_durations": durations
        }
    return d_duration


def get_connect_duration(mlinks):
    '''获取辅播总时长以及主播连麦时长'''
    s_duration = 0
    m_connect_duration = 0
    if len(mlinks) == 1:
        m_connect_duration = mlinks[0].get("sum_channel_duration")
        s_duration = m_connect_duration
    if len(mlinks) >= 2:
        try:
            mlinks = sorted(mlinks, key=lambda item: item.get("start_timestamp"))
            earliest_time = mlinks[0].get("start_timestamp")
            duration = mlinks[0].get("sum_channel_duration")
            latest_time = earliest_time + duration
            m_connect_duration = duration
            s_duration = duration
            for link in mlinks[1:]:
                start_timestamp = link.get("start_timestamp")
                duration = link.get("sum_channel_duration")
                s_duration += duration
                if start_timestamp >= latest_time:
                    m_connect_duration += duration
                    latest_time = start_timestamp + duration
                else:
                    if latest_time - start_timestamp < duration:
                        m_connect_duration = m_connect_duration + \
                            duration - (latest_time - start_timestamp)
                        latest_time = start_timestamp + duration
        except Exception as e:
            logger.info(e)
            logger.info(mlinks)
    return s_duration, m_connect_duration

@timing
def statistics_link_duration(collection):
    query = {"time": {"$gte": start, "$lte": start + 3600}, "level": 1, "ip": {"$nin": abroad_link_direct_ips}}
    sort = [("time", 1)]
    res = {"_id": 0}
    d_norepeat = {}
    for result in collection.find(query, res, sort=sort):
        ip = result["ip"]
        start_stamp = result["time"]
        if not (ip + str(start_stamp)) in d_norepeat:
            d_norepeat[ip + str(start_stamp)] = 1
            channels = result["channels"]
            handle_channels(ip, start_stamp, channels, link_data)
    link_duration = get_link_ms_duration(link_data)
    return link_duration

@timing
def statistics_msc_duration(collection):
    query = {"time": {"$gte": start, "$lte": start + 3600}, "level": 6, "ip": {"$nin": abroad_link_direct_ips}}
    sort = [("time", 1)]
    res = {"_id": 0}
    d_norepeat = {}
    for result in collection.find(query, res, sort=sort):
        ip = result["ip"]
        start_stamp = result["time"]
        if not (ip + str(start_stamp)) in d_norepeat:
            d_norepeat[ip + str(start_stamp)] = 1
            channels = result["channels"]
            handle_channels(ip, start_stamp, channels, msc_data)
    msc_duration = get_link_ms_duration(msc_data, is_msc=True)
    return msc_duration

@timing
def statistics_enc_duration(collection):
    query = {"time": {"$gte": start, "$lte": start + 3600}, "level": 2, "ip": {"$nin": abroad_link_direct_ips}}
    sort = [("time", 1)]
    res = {"_id": 0}
    d_norepeat = {}
    for result in collection.find(query, res, sort=sort):
        ip = result["ip"]
        start_stamp = result["time"]
        if not (ip + str(start_stamp)) in d_norepeat:
            d_norepeat[ip + str(start_stamp)] = 1
            channels = result["channels"]
            handle_enc_channels(start_stamp, channels)
    enc_duration = get_enc_duration(enc_data)
    return enc_duration


if __name__ == '__main__':
    getconfig()
    logger.info("statistics[start]: %s", datetime.datetime.now())
    subordinater_data = get_database(
        mongodb_subordinater_ip,
        mongodb_subordinater_port,
        mongodb_user,
        mongodb_pwd,
        mongodb_db)
    main_data = get_database(
        mongodb_main_ip,
        mongodb_main_port,
        mongodb_user,
        mongodb_pwd,
        mongodb_db)
    push_flow_data = subordinater_data.push_stream_info
    push_flow_statistics_hourly = main_data.push_flow_statistics_hourly
    d_link = statistics_link_duration(push_flow_data)
    d_msc = statistics_msc_duration(push_flow_data)
    d_enc = statistics_enc_duration(push_flow_data)
    s_channels = (set(d_link.keys()) | set(d_msc.keys()) | set(d_enc.keys()))
    l_inkegame = []
    for channel in s_channels:
        d_insert = {
            "time": start + 1800,
            "channel_group": channel
        }
        for d_type in [d_link, d_msc, d_enc]:
            if d_type.get(channel):
                d_insert.update(d_type[channel])

        if d_insert.get("connect_durations") and d_insert.get("link_mdurations") and d_insert.get("link_sdurations"):
            d_insert["common_durations"] = d_insert["link_mdurations"] + d_insert["link_sdurations"] - d_insert["connect_durations"]

        if channel in ("inkegame1", "inkegame2", "inkegame3"):
            l_inkegame.append(d_insert)

        push_flow_statistics_hourly.insert_one(d_insert)
        logger.info(d_insert)

    d_inkegame = {
        "time": start + 1800,
        "channel_group": "inkegame",
        "link_mdurations": 0,
        "link_sdurations": 0,
        "msc_durations": 0,
        "connect_durations": 0,
        "common_durations": 0,
        "enc_durations": 0
    }
    for item in l_inkegame:
        d_inkegame["link_mdurations"] += item.get("link_mdurations", 0)
        d_inkegame["link_sdurations"] += item.get("link_sdurations", 0)
        d_inkegame["msc_durations"] += item.get("msc_durations", 0)
        d_inkegame["connect_durations"] += item.get("connect_durations", 0)
        d_inkegame["common_durations"] += item.get("common_durations", 0)
        d_inkegame["enc_durations"] += item.get("enc_durations", 0)

    push_flow_statistics_hourly.insert_one(d_inkegame)
    logger.info(d_inkegame)
    print(d_inkegame)
    logger.info("statistics[end]: %s", datetime.datetime.now())
