#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time
from kafka import KafkaProducer
import threading
from multiprocessing import Process, Pool
from ftplib import FTP
ftp = FTP()
import re
import os
import socket
#import fcntl
import struct
import json
json.encoder.FLOAT_REPR = lambda x: format(x, '.2f')
import random
import signal
import logging.handlers
import logging
import configparser

code_version = "ICSAgent V3.8"
code_build = "2018082101"
fail_times = 0
last_fail_time = time.time()

cp = configparser.ConfigParser()
cp.read('producer.conf')
use_stable_ip = cp.getint('setting', 'use_stable_ip')
server_ip = cp.get('setting', 'server_ip')
log_type = cp.getint('setting', 'log_type')
log_duration = cp.getint('setting', 'log_duration')
code_name = cp.get('setting', 'code_name')
pzt_dir = cp.get('setting', 'pzt_dir')
kafka_addr = cp.get('setting', 'kafka_addr').split(",")
log_dir = cp.get('setting', 'log_dir')
rs_log_dir = cp.get('setting', 'rs_log_dir')


log_file = 'info.log'
handler = logging.handlers.RotatingFileHandler(
    log_file, maxBytes=5 * 1024 * 1024, backupCount=5)  # 实例化handler
fmt = '%(asctime)s - %(filename)s:%(lineno)s - %(name)s - %(message)s'
formatter = logging.Formatter(fmt)  # 实例化formatter
handler.setFormatter(formatter)  # 为handler添加formatter
logger = logging.getLogger('kafka_producer')  # 获取名为tst的logger
logger.addHandler(handler)  # 为logger添加handler
logger.setLevel(logging.DEBUG)

try:
    from hashlib import md5
    m = md5()
    a_file = open(code_name, 'rb')
    m.update(a_file.read())
    a_file.close()
    md5_str = m.hexdigest()
except BaseException:
    md5_str = "unknown"

if use_stable_ip != 1:
    try:
        in_ip_re = re.compile(
            r"(10\..+)|(172\.((1[6-9])|(2[0-9])|(3[0-1]))\..+)|(192\.168\..+)")
        server_ip = "unknown"
        ips = os.popen(
            "LANG=C ifconfig | grep \"inet addr\" | grep -v \"127.0.0.1\" |grep -v \"0.0.0.0\"| awk -F \":\" '{print $2}' | awk '{print $1}'").readlines()
        for ip in ips:
            ip = ip.replace("\n", "")
            if not in_ip_re.match(ip):
                server_ip = ip
                break
    except BaseException:
        server_ip = "unknown"


class TimeOutException(Exception):
    pass


def ifjam(u):
    seg_mode_time = 4 if u["seg_t"] else 10
    return (u["end"] - u["start"] - (u["seg_e"] - u["seg_s"])
            * seg_mode_time) > seg_mode_time


def stringtify_user_obj(u):
    channel_s = ""
    rate_s = ""
    for c in u['channel_n']:
        channel_s = channel_s + c + ':' + str(u['channel_n'][c]) + ','
    for r in u['rate_n']:
        rate_s = rate_s + r + ':' + str(u['rate_n'][r]) + ','
    # { liupan modify, 2018/3/21
    # return
    # str(u['u_ip'])+'_'+str(u['flu'])+'_'+str(u['start'])+'_'+str(u['end'])+'_'+str(u['jam'])+'_'+str(u['req_n'])+'_'+str(u['suc_n'])+'_'+rate_s+'_'+channel_s
    agent = ''      # CNTV使用，但为了保持数据格式同步，需要填充空字符串
    am = ''         # CNTV使用，但为了保持数据格式同步，需要填充空字符串
    btb = ''        # CNTV使用，但为了保持数据格式同步，需要填充空字符串
    channelno = ''  # CNTV使用，但为了保持数据格式同步，需要填充空字符串
    domain_s = ""
    for d in u['domain_n']:
        domain_s = domain_s + d + ':' + str(u['domain_n'][d]) + ','
        # freeze_avg_iv delalyed_avg
    str_duration = '_' + str(u['duration'])
    str_jam_all = '_' + str(u['jam_all'])
    str_freeze_avg_iv = '_' + str(u['freeze_avg_iv'])
    str_delalyed_avg = '_' + str(u['delalyed_avg'])
    str_return = str(u['u_ip']) + '_' + str(u['flu']) + '_' + str(u['start']) + '_' + str(u['end']) + '_' + \
        str(u['jam']) + '_' + str(u['req_n']) + '_' + str(u['suc_n']) + '_' + rate_s + '_' + channel_s + \
        '_' + agent + '_' + am + '_' + btb + '_' + channelno + '_' + domain_s
    str_return += str_freeze_avg_iv + str_delalyed_avg + str_duration + str_jam_all
    return str_return
    # } liupan modify, 2018/3/21


def conn_kafka(user_list, log_info, log_state, user_state):
    random.shuffle(kafka_addr)
    producer = None
    # find an available broker
    for broker in kafka_addr:
        try:
            producer = KafkaProducer(bootstrap_servers=broker)
            logger.info("connected to broker: %s", broker)
            break
        except Exception as e:
            logger.debug(str(Exception) + ":" + str(e))
    if producer is not None:
        if not log_state:
            try:
                res_log = producer.send("logs", log_info)
                time.sleep(5)
                if res_log.is_done:
                    log_state = True
            except Exception as e:
                log_state = False
        if not user_state:
            try:
                res_user = producer.send("users", user_list)
                time.sleep(5)
                if res_user.is_done:
                    user_state = True
            except BaseException:
                user_state = False
        producer.close()
    else:
        logger.debug("no broker available")

    return (log_state, user_state)


def calculate(file):
    start = file[7:21]
    starttm = int(time.mktime((int(start[0:4]), int(start[4:6]), int(
        start[6:8]), int(start[8:10]), int(start[10:12]), int(start[12:14]), 0, 0, 0)))

    logger.info("start analyzing: %s", file)
    req_re = re.compile(r"^(.+)(\d)_/seg(\d).+(\d{9})")
    # { liupan modify, 2018/3/21
    # live_re = re.compile(r"^(.*)/live/(ld/flv|ld/trans|flv|trans)/")
    live_re = re.compile(
        r"^(.*)/live/(ld/flv|ld/trans|fd/flv|fd/trans|flv|trans)/")
    channel_re = re.compile(
        r"/live/(ld/flv|ld/trans|fd/flv|fd/trans|flv|trans)/(.+)")
    # } liupan modify, 2018/3/21
    long_rate_re = re.compile(r'(\d+)_(\d+)')
    logs = open(log_dir + "/" + file, 'r').readlines()

    # init top_list
    top_list = {
        'ld/flv': {
            'type': 2,
            'list': [],
            'users': {},
            "req_n": 0,
            "suc_n": 0,
            "suc_r": 0,
            "user_n": 0,
            "jam_n": 0,
            "freeze_r": 0,
            "flu": 0,
            "band": 0,
            "rate_n": {},
            "bitrate": 0,
            "channel_n": {}
        },
        'ld/trans': {
            'type': 2,
            'list': [],
            'users': {},
            "req_n": 0,
            "suc_n": 0,
            "suc_r": 0,
            "user_n": 0,
            "jam_n": 0,
            "freeze_r": 0,
            "flu": 0,
            "band": 0,
            "rate_n": {},
            "bitrate": 0,
            "channel_n": {}
        },
        # { liupan add, 2018/3/21
        'fd/flv': {
            'type': 2,
            'list': [],
            'users': {},
            "req_n": 0,
            "suc_n": 0,
            "suc_r": 0,
            "user_n": 0,
            "jam_n": 0,
            "freeze_r": 0,
            "flu": 0,
            "band": 0,
            "rate_n": {},
            "bitrate": 0,
            "channel_n": {}
        },
        'fd/trans': {
            'type': 2,
            'list': [],
            'users': {},
            "req_n": 0,
            "suc_n": 0,
            "suc_r": 0,
            "user_n": 0,
            "jam_n": 0,
            "freeze_r": 0,
            "flu": 0,
            "band": 0,
            "rate_n": {},
            "bitrate": 0,
            "channel_n": {}
        },
        # } liupan add, 2018/3/21
        'flv': {
            'type': 2,
            'list': [],
            'users': {},
            "req_n": 0,
            "suc_n": 0,
            "suc_r": 0,
            "user_n": 0,
            "jam_n": 0,
            "freeze_r": 0,
            "flu": 0,
            "band": 0,
            "rate_n": {},
            "bitrate": 0,
            "channel_n": {}
        },
        'trans': {
            'type': 2,
            'list': [],
            'users': {},
            "req_n": 0,
            "suc_n": 0,
            "suc_r": 0,
            "user_n": 0,
            "jam_n": 0,
            "freeze_r": 0,
            "flu": 0,
            "band": 0,
            "rate_n": {},
            "bitrate": 0,
            "channel_n": {}
        },
    }
    total = {
        'user_list': [],
        'req_n': 0,
        'suc_n': 0,
        'jam_n': 0,
        'flu': 0,
        'band': 0,
        'rate_n': {},
        'channel_n': {},
        'duration': 0,
        'delayed_avg': 0,
        'freeze_avg_iv': 0,
        'jam_all': 0
    }

    # add by qjk

    for key in top_list:
        top_list[key]['duration'] = 0  # 拉流时长累加器
        top_list[key]['delayed_avg'] = 0  # 平均延时累加器
        top_list[key]['freeze_avg_iv'] = 0  # 平均卡顿间隔累加器
        top_list[key]['jam_all'] = 0  # 卡顿次数累加器

    # format logs
    for l in logs:
        try:
            #agent = l.split('"')[1].decode("utf-8",'ignore')
            agent = l.split('"')[1]
        except BaseException:
            continue
        try:
            x_group = l.split(" ")
            # 0Begin_Time, 1User_IP, 2ResponseCode, 3Flu, 4Duration,
            # 5Freeze_Count, 6Bitrate, 7Domain, 8Port, 9URI, 10UserAgent
            if len(x_group) < 11:
                continue
            ip = x_group[1]
            tim = int(x_group[0])
            status = bool(re.compile(r"^(2|3)\d{2}$").match(x_group[2]))
            flu = int(x_group[3])
            duration = int(x_group[4])
            live_ma = live_re.match(x_group[9])
            # { liupan modify, 2018/3/21
            # channel = x_group[7].replace('.','')
            domain = x_group[7].replace('.', '')
            # } liupan modify, 2018/3/21
            if live_ma:
                type = live_ma.group(2)
                rate = x_group[6]
                try:
                    live_jam = int(x_group[5]) > 0
                    jam_n = int(x_group[5])  # 卡顿数
                except BaseException:
                    live_jam = False
                    jam_n = 0
                # { liupan add, 2018/3/21
                channel_ma = channel_re.match(x_group[9])
                # { liupan modify, 2018/4/11
                # channel = channel_ma.group(2).replace('_', '.')
                channel = channel_ma.group(2).replace('_', '%')
                # } liupan modify, 2018/4/11
                # } liupan add, 2018/3/21
                # { liupan modify, 2018/3/21
                # r = (ip+agent,tim,status,channel,rate,"",live_jam,ip,agent,flu,duration)
                #r = (ip + agent, tim, status, channel, rate, "", live_jam, ip, agent, flu, duration, domain)
                # } liupan modify, 2018/3/21

                # 对原始log中增加卡顿时长和平均延时字段的处理
                r = [
                    ip + agent,
                    tim,
                    status,
                    channel,
                    rate,
                    "",
                    live_jam,
                    ip,
                    agent,
                    flu,
                    duration,
                    domain]

                # add by qjk
                if len(x_group) > 12:  # 对老日志的支持
                    freeze_t = int(x_group[11])  # 卡顿时长
                    delayed_avg = int(x_group[12])  # 平均延时
                else:
                    freeze_t = 0
                    delayed_avg = 0

                r.append(freeze_t)
                r.append(delayed_avg)
                r.append(jam_n)

                # if top_list.has_key(type):
                if type in top_list:
                    top_list[type]['list'].append(r)
        except BaseException:
            pass

    # analyze top_list
    for category_name in top_list:
        current_category = top_list[category_name]
        log_list = current_category['list']
        user_list = current_category['users']
        rate_list = current_category['rate_n']
        channel_list = current_category['channel_n']

        if current_category['type'] == 2:
            for l in log_list:
                # if user_list.has_key(l[0]):
                if l[0] in user_list:
                    user_list[l[0]]["req_n"] += 1
                    if l[2]:
                        user_list[l[0]]["suc_n"] += 1
                    user_list[l[0]]["flu"] += l[9]
                    user_list[l[0]]["duration"] += l[10]  # add by qjk
                    # { liupan add, 2018/5/31
                    if l[6]:
                        user_list[l[0]]["jam"] += 1

                    # } liupan add, 2018/5/31

                    # add by qjk
                    user_list[l[0]]["delalyed_avg"] += l[13]  # 累加平均延时
                    user_list[l[0]]["jam_all"] += l[14]  # 累加卡顿次数
                else:
                    user_list[l[0]] = {
                        "u_ip": l[7],
                        "req_n": 1,
                        "suc_n": 1 if l[2] else 0,
                        "start": l[1],
                        "end": l[1],
                        "agent": l[8],
                        # { liupan modify, 2018/5/31
                        # "jam": l[6],
                        "jam": 1 if l[6] else 0,
                        # } liupan modify, 2018/5/31
                        "flu": l[9],
                        "duration": l[10],
                        "rate_n": {},
                        "channel_n": {},
                        # { liupan modify, 2018/3/21
                        "domain_n": {},
                        # } liupan modify, 2018/3/2
                        "type": category_name,
                        # add by qjk
                        "delalyed_avg": l[13],  # 平均延时
                        "jam_all": l[14],  # 卡顿次数
                        "freeze_avg_iv": 0  # 平均卡顿间隔
                    }

                # if channel_list.has_key(l[3]):
                if l[3] in channel_list:
                    channel_list[l[3]] += l[9]
                else:
                    channel_list[l[3]] = l[9]
                # if total['channel_n'].has_key(l[3]):
                if l[3] in total['channel_n']:
                    total['channel_n'][l[3]] += l[9]
                else:
                    total['channel_n'][l[3]] = l[9]
                # if user_list[l[0]]['channel_n'].has_key(l[3]):
                if l[3] in user_list[l[0]]['channel_n']:
                    user_list[l[0]]['channel_n'][l[3]] += l[9]
                else:
                    user_list[l[0]]['channel_n'][l[3]] = l[9]
                # { liupan add, 2018/3/21
                # if user_list[l[0]]['domain_n'].has_key(l[11]):
                if l[11] in user_list[l[0]]['domain_n']:
                    user_list[l[0]]['domain_n'][l[11]] += l[9]
                else:
                    user_list[l[0]]['domain_n'][l[11]] = l[9]
                # } liupan add, 2018/3/21

                lrms = long_rate_re.findall(l[4])
                for lrm in lrms:
                    k = lrm[0]
                    # if rate_list.has_key(k):
                    if k in rate_list:
                        rate_list[k] += int(lrm[1])
                    else:
                        rate_list[k] = int(lrm[1])
                    # if user_list[l[0]]['rate_n'].has_key(k):
                    if k in user_list[l[0]]['rate_n']:
                        user_list[l[0]]['rate_n'][k] += int(lrm[1])
                    else:
                        user_list[l[0]]['rate_n'][k] = int(lrm[1])

                if l[2]:
                    current_category['suc_n'] += 1
                # flu total
                current_category['flu'] += l[9]
            for u in user_list:
                # { liupan modify, 2018/5/31
                # if user_list[u]["jam"]:
                #     current_category['jam_n'] += 1
                current_category['jam_n'] += user_list[u]["jam"]
                # } liupan modify, 2018/5/31

                # add by qjk
                # 统计当前协议的平均延时，拉流时长，卡顿次数和，用于平均值计算
                current_category['delayed_avg'] += user_list[u]["delalyed_avg"]
                current_category['duration'] += user_list[u]["duration"]
                current_category['jam_all'] += user_list[u]["jam_all"]
                # 计算当前用户的平均卡顿间隔和平均延时时长
                if user_list[u]["jam_all"] != 0:
                    user_list[u]["freeze_avg_iv"] = round(
                        float(user_list[u]["duration"]) / user_list[u]["jam_all"], 2)
                if user_list[u]['req_n'] != 0:
                    user_list[u]["delalyed_avg"] = round(
                        float(user_list[u]["delalyed_avg"]) / user_list[u]['req_n'], 2)

        current_category['req_n'] = len(log_list)
        current_category['user_n'] = len(user_list)
        if current_category['req_n'] != 0:
            current_category['suc_r'] = round(
                float(current_category['suc_n'] * 100) / current_category['req_n'], 2)
        # { liupan modify, 2018/5/31
        # if len(user_list)!=0:
        #     current_category['freeze_r'] = round(float(current_category['jam_n']*100)/len(user_list),2)
            current_category['freeze_r'] = round(
                float(current_category['jam_n'] * 100) / current_category['req_n'], 2)
        # } liupan modify, 2018/5/31
        current_category['band'] = round(
            float(current_category['flu']) * 8 / log_duration / 1000, 2)
        try:
            current_category['bitrate'] = (
                rate_list["0"] * 4000 + rate_list["1"] * 2000 + rate_list["2"] * 1500 + rate_list["3"] * 850 + rate_list["4"] * 500) / (
                rate_list["1"] + rate_list["2"] + rate_list["3"] + rate_list["4"])
        except BaseException:
            current_category['bitrate'] = 0

        # to total
        total['user_list'].extend(
            list(map(stringtify_user_obj, user_list.values())))
        total['req_n'] += current_category['req_n']
        total['suc_n'] += current_category['suc_n']
        total['jam_n'] += current_category['jam_n']
        total['flu'] += current_category['flu']
        total['band'] += current_category['band']
        for rate in current_category['rate_n']:
            # if total['rate_n'].has_key(rate):
            if rate in total['rate_n']:
                total['rate_n'][rate] += current_category['rate_n'][rate]
            else:
                total['rate_n'][rate] = current_category['rate_n'][rate]

        # add by qjk
        if 'duration' in current_category:
            total['duration'] += current_category['duration']
        if 'delayed_avg' in current_category:
            total['delayed_avg'] += current_category['delayed_avg']
        if 'jam_all' in current_category:
            total['jam_all'] += current_category['jam_all']

        # clear
        del current_category['type']
        del current_category['list']
        del current_category['users']

    # add total keys
    user_list = total['user_list']
    for category_name, category_data in top_list.items():
        if category_data.get("jam_all"):
            category_data["freeze_avg_iv"] = round(
                float(category_data["duration"]) / category_data["jam_all"], 2)
        if category_data.get("req_n"):
            category_data["delayed_avg"] = round(
                float(category_data["delayed_avg"]) / category_data["req_n"], 2)

    log_info = top_list
    log_info['from'] = log_type
    log_info['version'] = code_version + ' ' + code_build
    log_info['log_duration'] = log_duration
    log_info['md5'] = md5_str
    log_info['s_ip'] = server_ip
    log_info['start'] = starttm
    log_info['req_n'] = total['req_n']
    log_info['suc_n'] = total['suc_n']
    log_info['jam_all'] = total['jam_all']
    log_info['duration'] = total['duration']
    if total['req_n'] != 0:
        log_info['suc_r'] = round(
            float(total['suc_n'] * 100) / total['req_n'], 2)
    log_info['user_n'] = len(user_list)
    log_info['jam_n'] = total['jam_n']
    # { liupan modify, 2018/5/31
    # if len(user_list)!=0:
    #     log_info['freeze_r'] = round(float(total['jam_n']*100)/len(user_list),2)
    if total['req_n'] != 0:
        log_info['freeze_r'] = round(
            float(total['jam_n'] * 100) / total['req_n'], 2)
    # } liupan modify, 2018/5/31
    log_info['flu'] = total['flu']
    log_info['band'] = total['band']
    log_info['rate_n'] = total['rate_n']
    try:
        log_info['bitrate'] = 0
        total_rate = 0
        total_time = 0
        for rate in total['rate_n']:
            total_rate += int(rate) * int(total['rate_n'][rate])
            total_time += float(total['rate_n'][rate])
        log_info['bitrate'] = round(total_rate / total_time, 2)
    except BaseException:
        log_info['bitrate'] = 0
    log_info['channel_n'] = total['channel_n']

    # add by qjk
    # 计算平均延时
    if total['req_n'] != 0:
        log_info['delayed_avg'] = round(
            float(total['delayed_avg']) / total['req_n'], 2)
    else:
        log_info['delayed_avg'] = 0

    if total['jam_all'] != 0:
        log_info['freeze_avg_iv'] = round(
            float(total['duration']) / total['jam_all'], 2)
    else:
        log_info['freeze_avg_iv'] = 0

    # send to kafka
    user_list_json = json.JSONEncoder().encode({
        'log_time': starttm,
        'from': log_type,
        's_ip': server_ip,
        'users': user_list
    })
    log_info_json = json.JSONEncoder().encode(log_info)

    retry_time = 3
    log_state = False
    user_state = False
    global fail_times
    global last_fail_time
    while retry_time > 0:
        retry_time -= 1
        logger.info("user_list_json: %s", user_list_json)
        logger.info("log_info_json: %s", log_info_json)
        res = conn_kafka(user_list_json, log_info_json, log_state, user_state)
        log_state = res[0]
        user_state = res[1]
        if log_state and user_state:
            logger.info("complete analyzing: %s", file)
            break
        time.sleep(5)
    if retry_time == 0:
        if time.time() - last_fail_time > 600:
            fail_times = 0
        else:
            last_fail_time = time.time()
        if fail_times > 10:
            logger.error("kill myself")
            os._exit(0)
        else:
            logger.error("Kafka error and retry failed")
            fail_times += 1
            raise TimeOutException()


def handler(signum, frame):
    logger.error("Log Timeout")
    raise TimeOutException()

def rs_folder_log(log_dir):
    """判断rs日志目录是否存在，是否有内容"""
    if os.path.exists(log_dir) and os.listdir(log_dir):
        rs_folder = set([_f[2] for _f in os.walk(log_dir)][0])
    else:
        rs_folder = set()
    return rs_folder


def monitor():
    pzs_dir = log_dir
    pzs_origin = set([_f[2] for _f in os.walk(pzs_dir)][0])
    rs_origin = rs_folder_log(rs_log_dir)
    p = Pool(processes=1)
    while True:
        time.sleep(3)
        pzs_final = set([_f[2] for _f in os.walk(pzs_dir)][0])
        rs_final = rs_folder_log(rs_log_dir)
        pzs_dif = pzs_final.difference(pzs_origin)
        rs_dif = rs_final.difference(rs_origin)
        pzs_origin = pzs_final
        rs_origin = rs_final
        while len(pzs_dif) > 0 or len(rs_dif) > 0:
            if len(pzs_dif) > 0:
                pzs_file = pzs_dif.pop()
                p.apply_async(handle_pzs_log, args=(pzs_file, ))
                # Process(target=handle_pzs_log, args=(pzs_file,)).start()
            if len(rs_dif) > 0:
                rs_file = rs_dif.pop()
                handle_rs_log(rs_file)



def handle_pzs_log(added_file):
    logger.info('parent process: %s', os.getppid())  # 父进程号
    logger.info('process id:%s', os.getpid())   # 进程号
    if re.compile(r"^access_.+log$").match(added_file):
        err_try_time = 0
        try:
            signal.signal(signal.SIGALRM, handler)
            signal.alarm(50)
            time.sleep(random.randint(0, 10))
            calculate(added_file)
            error_files = open(pzt_dir + "timeout_logs", 'w+').readlines()
            while len(error_files) > 0:
                err_file = error_files.pop(0)
                open(pzt_dir + "timeout_logs", 'w+').writelines(error_files)
                err_f_ma = re.compile(
                    r"^(access_.+log):(\d).+").match(err_file)
                if err_f_ma:
                    added_file = err_f_ma.group(1)
                    err_try_time = int(err_f_ma.group(2))
                    if err_try_time < 9:
                        err_try_time += 1
                        try:
                            calculate(added_file)
                        except BaseException:
                            logger.error("File: %s doesn't exist", added_file)
            signal.alarm(0)
        except TimeOutException as e:
            try:
                add_f = open(
                    pzt_dir + "timeout_logs", 'w+').readlines()
                add_f.append(added_file + ":" + str(err_try_time) + "\n")
                open(pzt_dir + "timeout_logs", 'w+').writelines(add_f)
            except BaseException:
                logger.error("add timeout file error")
        except Exception as e:
            logger.error(str(Exception) + ":" + str(e) + str(e.args))


def handle_rs_log(added_file):
    pass


class TimeoutError(Exception):
    pass





if __name__ == "__main__":
    logger.info("start...%s", server_ip)
    try:
        monitor()
    except BaseException:
        logger.error("Init fail")