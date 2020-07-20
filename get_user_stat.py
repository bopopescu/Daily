#!/usr/bin/env python
# -*- coding: utf-8 -*-

#import re
import io
import string
import os
import time
import json
from ip_list import *
from ctypes import *
import socket
import struct
import logging
import logging.handlers
import multiprocessing
import pickle
import configparser
import queue
from multiprocessing import Array
from pymongo import MongoClient
# } liupan add, 2017/8/1

# statis_version='CNTV';
# statis_version='LD_KW';
statis_version = 'JAPAN'

region_all = []
operator_all = []
cdn_all = []
agent_cdn = []
domain_channel_flag = 0
bitrate_detail = {}
domain_channel = {}
user_region_flag = 0
region_dic = {}
process_num = 0
mongodb_main_ip = ''
mongodb_main_port = 27017
mongodb_subordinater_ip = ''
mongodb_subordinater_port = 27017
mongodb_db = ''
mongodb_user = ''
mongodb_pwd = ''
push_stream_stability_flag = 0
push_stream_stability_delta_min = {}
push_stream_stability_delta_max = {}
push_stream_stability_delta_change = {}


def getVersionConfig():
    global statis_version
    global mongodb_main_ip
    global mongodb_main_port
    global mongodb_subordinater_ip
    global mongodb_subordinater_port
    global mongodb_db
    # { liupan add, 2017/8/1
    global region_all
    global operator_all
    global cdn_all
    global agent_cdn
    global domain_channel_flag
    global user_region_flag
    global region_dic
    global process_num
    # } liupan add, 2017/8/1
    # { liupan add, 2018/4/24
    global mongodb_user
    global mongodb_pwd
    # } liupan add, 2018/4/24
    # { liupan add, 2018/6/14
    global push_stream_stability_flag
    global push_stream_stability_delta_min
    global push_stream_stability_delta_max
    global push_stream_stability_delta_change
    # } liupan add, 2018/6/14
    cp = configparser.ConfigParser()
    cp.read('version.conf')
    statis_version = cp.get('setting', 'version')
    mongodb_main_ip = cp.get('setting', 'mongodb_main_ip')
    mongodb_main_port = cp.getint('setting', 'mongodb_main_port')
    mongodb_subordinater_ip = cp.get('setting', 'mongodb_subordinater_ip')
    mongodb_subordinater_port = cp.getint('setting', 'mongodb_subordinater_port')
    mongodb_db = cp.get('setting', 'mongodb_db')
    # { liupan add, 2018/4/24
    mongodb_user = cp.get('setting', 'mongodb_user')
    mongodb_pwd = cp.get('setting', 'mongodb_pwd')
    # } liupan add, 2018/4/24

    # { liupan add, 2017/8/1
    region_all = cp.get('setting', 'regions').split(',')
    region_all.append('其他')
    operator_all = cp.get('setting', 'operators').split(',')
    operator_all.append('其他')
    cdn_all.append('qt')
    cdn_all += cp.get('setting', 'cdns').split(',')
    # cdn_all.append('qt');
    agent_cdn = cp.get('setting', 'agent_cdns').split(',')
    domain_channel_flag = cp.getint('setting', 'domain_channel_flag')
    user_region_flag = cp.getint('setting', 'user_region_flag')
    region_dic_temp = cp.get('setting', 'regions_code').split(',')
    for strx in region_dic_temp:
        str_key_value = strx.split(':')
        region_dic[str_key_value[0]] = str_key_value[1]

    process_num = cp.getint('setting', 'process_num')
    push_stream_stability_flag = cp.getint('setting', 'push_stream_stability_flag')
    config_tmp = cp.get('setting', 'push_stream_stability_delta_min')
    config_tmp = config_tmp.split(",")
    for x in config_tmp:
        x = x.split(":")
        if len(x) != 2:
            continue
        push_stream_stability_delta_min[x[0]] = int(x[1])

    config_tmp = cp.get('setting', 'push_stream_stability_delta_max')
    config_tmp = config_tmp.split(",")
    for x in config_tmp:
        x = x.split(":")
        if len(x) != 2:
            continue
        push_stream_stability_delta_max[x[0]] = int(x[1])

    config_tmp = cp.get('setting', 'push_stream_stability_delta_change')
    config_tmp = config_tmp.split(",")
    for x in config_tmp:
        x = x.split(":")
        if len(x) != 2:
            continue
        push_stream_stability_delta_change[x[0]] = int(x[1])


def read_bitrate_config_common():
    """读取码率的本地配置: 级别与码率值的映射关系"""
    cp = configparser.ConfigParser()
    cp.read('get_user_stat.conf')
    bitrate_list = {}
    for key, value in cp.items('bitrate'):
        bitrate_list[key] = int(value)

    return bitrate_list


def get_rate_n_detail_common(detail):
    """计算节点的码率信息:码率和、码率时间"""
    bitrate_temp = 0
    bitrate_time = 0

    for key, value in detail.items():
        bitrate_temp += bitrate_detail[key] * int(value)
        bitrate_time += int(value)

    ret_detail = {'bit_sum': bitrate_temp, 'bit_time': bitrate_time}
    return ret_detail
# } liupan add, 2017/8/1


def get_op(n_op):
    op_temp = "其他"
    if len(n_op) < 1 or n_op == '#':
        #print("op len < 1");
        return op_temp
    n_op_temp = int(n_op)
    # { liupan modify, 2017/8/25
    # if n_op_temp == 0 :
    #     return "联通";
    # elif n_op_temp==1:
    #     return "电信";
    # elif n_op_temp==2:
    #     return "移动";
    if n_op_temp >= 0 and n_op_temp < len(operator_all):
        return operator_all[n_op_temp]
    # } liupan modify, 2017/8/25
    else:
        return "其他"


def read_bitrate_config():
    """读取码率的本地配置(旧)"""
    cp = configparser.ConfigParser()
    cp.read('get_user_stat.conf')
    #print("has test:",cp.has_section('bitrate'));
    conf_list = []
    conf_list.append(cp.getint('bitrate', 'pd'))
    conf_list.append(cp.getint('bitrate', 'td'))
    conf_list.append(cp.getint('bitrate', 'ud'))
    conf_list.append(cp.getint('bitrate', 'hd'))
    conf_list.append(cp.getint('bitrate', 'md'))
    # print(conf_list);
    return conf_list


def read_ld_bitrate_config():
    """读取码率的本地配置(旧)"""
    cp = configparser.ConfigParser()
    cp.read('get_user_stat.conf')
    cdn_file_dic = {}
    for key, value in cp.items('bitrate'):
        cdn_file_dic[key] = int(value)
    # print(cdn_file_dic);
    return cdn_file_dic


def get_ip_detail(ip_temp, _proc, _op):
    """通过用户ip获得用户的地区、运营商"""
    # { liupan delete, 2017/8/1
    # region_dic={"BJ":"北京","SH":"上海","GD":"广东","TJ":"天津","AH":"安徽","CQ":"重庆","FJ":"福建","GS":"甘肃","GX":"广西","GZ":"贵州","HI":"海南","HE":"河北","HA":"河南","HL":"黑龙江","HB":"湖北","HN":"湖南","JL":"吉林","JS":"江苏","JX":"江西","LN":"辽宁","NM":"内蒙古","NX":"宁夏","QH":"青海","SD":"山东","SX":"山西","SN":"陕西","SC":"四川","TW":"台湾","XZ":"西藏","HK":"香港","MO":"澳门","XJ":"新疆","YN":"云南","ZJ":"浙江","QT":"其他"};
    # } liupan delete, 2017/8/1

    Country = create_string_buffer(1024)
    proc = create_string_buffer(1024)
    op_str = create_string_buffer(1024)
    _proc = "其他"
    _op = "其他"
    if '.' not in ip_temp:
        return [_proc, _op]
    num_ip = socket.ntohl(
        struct.unpack(
            "I", socket.inet_aton(
                str(ip_temp)))[0])
    ip = c_int(num_ip)

    ret = get_ip(ip, Country, proc, op_str)
    # print(Country.value.decode('utf-8'));
    # print(op_str.value.decode('utf-8'));
    # print(proc.value.decode('utf-8'));
    proc_temp = proc.value.decode('utf-8')
    # print("proc_temp:%s"%proc_temp)
    # print("ret:%d"%ret)

    if ret != 0:
        return -1

    # { liupan add, 2018/6/12
    c_str = Country.value.decode('utf-8')
    # } liupan add, 2018/6/12
    if proc_temp in region_dic:
        _proc = region_dic[proc_temp]
    # { liupan add, 2018/6/12
    elif c_str in region_dic:
        _proc = region_dic[c_str]
    # } liupan add, 2018/6/12
    # print(_proc);
    op_utf8 = op_str.value.decode('utf-8')
    _op = get_op(op_utf8)
    ret_detail = [_proc, _op]
    return ret_detail


def get_ip_detail2(ip_temp, _proc, _op):
    temp = ["其他", "其他"]
    return temp

# { liupan add, 2017/8/1


def get_user_table_detail_common(str_src):
    """获取原始用户信息: 返回dict"""
    src_list = str_src.split("_")
    dst_dic = {}
    dst_dic["u_ip"] = src_list[0]
    dst_dic['flu'] = int(src_list[1])
    if src_list[4] == "False":
        dst_dic['jam'] = 0
    # { liupan modify, 2018/5/31
    # else:
    #     dst_dic['jam'] = 1;
    elif src_list[4] == "True":
        dst_dic['jam'] = int(src_list[5])
    else:
        dst_dic['jam'] = int(src_list[4])
    # } liupan modify, 2018/5/31
    dst_dic['req_n'] = int(src_list[5])
    dst_dic['suc_n'] = int(src_list[6])

    rate_n = {}
    rate_list = src_list[7].split(",")
    for i in rate_list:
        rate_dic = []
        rate_dic = i.split(":")
        if len(rate_dic) >= 2:
            rate_n[rate_dic[0]] = int(rate_dic[1])
    dst_dic["rate_n"] = rate_n

    channel_info = {}
    # { liuapn modify, 2018/4/11
    # # { liupan modify, 2018/3/21
    # # channel_list = src_list[8].split(",");
    # channel_list = src_list[8].replace('.', '_').split(",");
    # # } liupan modify, 2018/3/21
    channel_list = src_list[8].replace('%', '_').split(",")
    # } liupan modify, 2018/4/11
    flu_sum = 0
    # { liupan add, 2018/6/14
    dst_dic['channelnames'] = {}
    # } liupan add, 2018/6/14
    for i in channel_list:
        channel_temp = i.split(":")
        if len(channel_temp) == 2:
            # { liupan modify, 2018/1/19
            # if domain_channel_flag == 1:
            #     if not(channel_temp[0] in domain_channel):
            #         channel_info['unkown'] = channel_temp[1];
            #     else:
            #         channel_info[domain_channel[channel_temp[0]]] = channel_temp[1];
            # else:
            #     channel_info[channel_temp[0]] = channel_temp[1];

            channel_name_t = 'unknown'
            if domain_channel_flag == 1:
                # { liupan modify, 2018/3/21
                # if not(channel_temp[0] in domain_channel):
                #     channel_name_t = 'unkown';
                # else:
                #     channel_name_t = domain_channel[channel_temp[0]];
                for channel_re in domain_channel:
                    # { liupan modify, 2018/3/29
                    # if channel_re.index('*') < 0:
                    if '*' not in channel_re:
                        # } liupan modify, 2018/3/29
                        if channel_re == channel_temp[0]:
                            channel_name_t = domain_channel[channel_re]
                            break
                        continue

                    test_re = re.compile(channel_re)
                    if test_re.match(channel_temp[0]):
                        channel_name_t = domain_channel[channel_re]
                        break
                # } liupan modify, 2018/3/21
            else:
                channel_name_t = channel_temp[0]

            # { liupan add, 2018/6/14
            dst_dic['channelnames'][channel_temp[0]] = channel_name_t
            # } liupan add, 2018/6/14

            if channel_name_t in channel_info:
                # { liupan modify, 2018/4/13
                # channel_info[channel_name_t] += channel_temp[1];
                channel_info[channel_name_t] += int(channel_temp[1])
                # } liupan modify, 2018/4/13
            else:
                # { liupan modify, 2018/4/13
                # channel_info[channel_name_t] = channel_temp[1];
                channel_info[channel_name_t] = int(channel_temp[1])
                # } liupan modify, 2018/4/13
            # } liupan modify, 2018/1/19
            flu_sum += int(channel_temp[1])
    dst_dic["channel_flu"] = channel_info
    channel_bit = {}
    for channel_name, channel_f in channel_info.items():
        channel_bit[channel_name] = {}
        for index_bit, bit in rate_n.items():
            if int(flu_sum) != 0:
                channel_bit[channel_name][index_bit] = int(
                    bit) * int(channel_f) / float(flu_sum)
    dst_dic['channel_bit'] = channel_bit

    # { liupan modify, 2018/3/21
    # if len(agent_cdn) > 0 and len(src_list) > 10 and src_list[10] != 'am':
    if len(agent_cdn) > 0 and len(
            src_list) > 10 and src_list[10] != 'am' and src_list[10] != '':
        # } liupan modify, 2018/3/21
        dst_dic['agent'] = src_list[9]
        dst_dic['am'] = int(src_list[10])
    else:
        dst_dic['agent'] = 'none'
        dst_dic['am'] = 0

    # { liupan add, 2017/8/17
    # { liupan modify, 2018/3/21
    # if len(agent_cdn) > 0 and len(src_list) > 11:
    if len(agent_cdn) > 0 and len(src_list) > 11 and src_list[11] != '':
        # } liupan modify, 2018/3/21
        dst_dic['sy'] = int(src_list[11])
    else:
        dst_dic['sy'] = 0
    # } liupan add, 2017/8/17

    # { liupan add, 2018/3/21
    if len(agent_cdn) > 0 and len(src_list) > 12 and src_list[12] != '':
        dst_dic['channelno'] = src_list[12]
    else:
        dst_dic['channelno'] = ""

    if len(agent_cdn) > 0 and len(src_list) > 13 and src_list[13] != '':
        dst_dic['domain_n'] = src_list[13]
    else:
        dst_dic['domain_n'] = ""
    # } liupan add, 2018/3/21


    if len(src_list) > 14 and src_list[14] != '':
        dst_dic['freeze_avg_iv'] = float(src_list[14])
    else:
        dst_dic['freeze_avg_iv'] = 0

    if len(src_list) > 15 and src_list[15] != '':
        dst_dic['delayed_avg'] = float(src_list[15])
    else:
        dst_dic['delayed_avg'] = 0

    # add by qjk
    if len(src_list) > 16 and src_list[16] != '':
        dst_dic['duration'] = float(src_list[16])
    else:
        dst_dic['duration'] = 0

    if len(src_list) > 17 and src_list[17] != '':
        dst_dic['jam_all'] = int(src_list[17])
    else:
        dst_dic['jam_all'] = 0

    return dst_dic
# } liupan add, 2017/8/1


def get_user_table_detail(str_src):
    src_list = str_src.split("_")
    dst_dic = {}
    dst_dic["u_ip"] = src_list[0]
    dst_dic['flu'] = int(src_list[1])
    if src_list[4] == "False":
        dst_dic['jam'] = 0
    else:
        dst_dic['jam'] = 1
    dst_dic['req_n'] = int(src_list[5])
    dst_dic['suc_n'] = int(src_list[6])

    rate_n = {}
    rate_list = src_list[7].split(",")
    for i in rate_list:
        rate_dic = []
        rate_dic = i.split(":")
        if len(rate_dic) >= 2:
            rate_n[rate_dic[0]] = int(rate_dic[1])
    dst_dic["rate_n"] = rate_n

    channel_info = {}
    channel_list = src_list[8].split(",")
    flu_sum = 0
    for i in channel_list:
        channel_temp = i.split(":")
        if len(channel_temp) == 2:
            channel_info[channel_temp[0]] = channel_temp[1]
            flu_sum += int(channel_temp[1])
    dst_dic["channel_flu"] = channel_info
    channel_bit = {}
    for channel_name, channel_f in channel_info.items():
        channel_bit[channel_name] = {}
        for index_bit, bit in rate_n.items():
            if int(flu_sum) != 0:
                channel_bit[channel_name][index_bit] = int(
                    bit) * int(channel_f) / float(flu_sum)
    dst_dic['channel_bit'] = channel_bit

    if len(src_list) > 10:
        if src_list[10] != 'am':
            dst_dic['agent'] = src_list[9]
            dst_dic['am'] = int(src_list[10])
        else:
            dst_dic['agent'] = 'none'
            dst_dic['am'] = 0
        # print(str_src);
    else:
        dst_dic['agent'] = 'none'
        dst_dic['am'] = 0
    return dst_dic


def get_user_table_detail_ld(str_src, domain_channel):
    src_list = str_src.split("_")
    dst_dic = {}
    dst_dic["u_ip"] = src_list[0]
    dst_dic['flu'] = int(src_list[1])
    if src_list[4] == "False":
        dst_dic['jam'] = 0
    else:
        dst_dic['jam'] = 1
    dst_dic['req_n'] = int(src_list[5])
    dst_dic['suc_n'] = int(src_list[6])

    rate_n = {}
    rate_list = src_list[7].split(",")
    for i in rate_list:
        rate_dic = []
        rate_dic = i.split(":")
        if len(rate_dic) >= 2:
            rate_n[rate_dic[0]] = int(rate_dic[1])
    dst_dic["rate_n"] = rate_n

    channel_info = {}
    channel_list = src_list[8].split(",")
    flu_sum = 0
    for i in channel_list:
        channel_temp = i.split(":")
        if len(channel_temp) == 2:
            if not(channel_temp[0] in domain_channel):
                channel_info['unkown'] = channel_temp[1]
            else:
                channel_info[domain_channel[channel_temp[0]]] = channel_temp[1]
            flu_sum += int(channel_temp[1])
    dst_dic["channel_flu"] = channel_info
    channel_bit = {}
    for channel_name, channel_f in channel_info.items():
        channel_bit[channel_name] = {}
        for index_bit, bit in rate_n.items():
            if int(flu_sum) != 0:
                channel_bit[channel_name][index_bit] = int(
                    bit) * int(channel_f) / float(flu_sum)
    dst_dic['channel_bit'] = channel_bit

    dst_dic['agent'] = 'none'
    dst_dic['am'] = 0
    return dst_dic


def read_domain_channel_config():
    cp = configparser.ConfigParser()
    cp.read('get_user_stat.conf')
    cdn_file_dic = {}
    for key, value in cp.items('domain_channel'):
        cdn_file_dic[key] = value
    # print(cdn_file_dic);
    return cdn_file_dic

# { liupan add, 2018/1/18


def getAgentName(am, agent):
    return agent


def isNewAgent(am, agent):
    if agent == 'other':
        return False

    am_int = am % 10
    if am_int == 2 or am_int == 3 or am_int == 4 or am_int == 5 or am_int == 6:
        return True

    return False
# } liupan add, 2018/1/18

# { liupan add, 2018/6/14


def isChannelPushFreeze(channel_name, start_time):
    subordinate_db = subordinate_client.get_database("monitor_db")
    push_stream_coll = subordinate_db.push_stream_info

    channel_group = ""
    for x in push_stream_stability_delta_change:
        xt = x + "/"
        if xt in channel_name:
            channel_group = x
            break

    if channel_group == "":
        return False

    channel_name_link = channel_name.split("/")
    channel_name_link = channel_name_link[len(channel_name_link) - 1]
    channel_name_link = channel_group + "/mlinkm/" + channel_name_link

    pipeline = [{'$unwind': "$channels"},
             {'$match': {'time': {'$gte': start_time - 180,
                                  '$lte': start_time},
                         'channels.channel_name': channel_name_link}},
             {'$project': {'_id': 0,
                           'channels.delta': 1}}]

    results = push_stream_coll.aggregate(pipeline)
    maxDelta = -99999999
    minDelta = 99999999
    for result in results:
        if maxDelta < result['channels']['delta']:
            maxDelta = result['channels']['delta']
        if minDelta > result['channels']['delta']:
            minDelta = result['channels']['delta']

    if maxDelta <= push_stream_stability_delta_min[channel_group]:
        return True
    if minDelta >= push_stream_stability_delta_max[channel_group]:
        return True
    if maxDelta - \
            minDelta >= push_stream_stability_delta_change[channel_group]:
        return True

    return False
# } liupan add, 2018/6/14


def get_test_data():
    result_list = [{
        "from": 1,
        "users": [
            "222.168.31.249_523339_1530603600_1530603600_0_2_2_64:60,_production/mlinks1%41362/31633:523339,_____pzlink34powzamediacom:523339,_0_0.0",
            "112.42.207.164_1432363_1530603600_1530603600_1_1_1_819:59,_production/mlinkm/31627:1432363,_____pzlink34powzamediacom:1432363,_5873.4_0.0",
            "180.106.127.86_6696180_1530603540_1530603540_1_2_2_819:60,_production/mlinkm/31315:6696180,_____pzlink34powzamediacom:6696180,_60128.0_0.0",
            "101.246.184.199_296272_1530603600_1530603600_1_2_2_64:64,_production/mlinks1%41339/31627:296272,_____pzlink34powzamediacom:296272,_7100.44_0.0",
            "1.57.11.207_519167_1530603600_1530603600_0_1_1_64:60,_production/mlinks1%41053/31315:519167,_____pzlink34powzamediacom:519167,_0_0.0",
            "223.104.108.205_9461426_1530603540_1530603540_0_2_2_819:61,_production/mlinkm/31633:9461426,_____pzlink34powzamediacom:9461426,_0_0.0"
        ],
        "log_time": 1530605100,
        "s_ip": "127.0.0.1"
    }]
    return result_list

# { liupan modify, 2017/8/1
# def nuser_stat_process(result_list,ip_dic,vip_list,p_queue):


def nuser_stat_process(
        result_list,
        ip_dic,
        vip_list,
        p_queue,
        queue_statistic,
        queue_history_single,
        queue_history_sum,
        queue_channel_sum,
        queue_history_user_by_node,
        history_single_count,
        channel_sum_count,
        history_user_by_node_count,
        count_index):
    # } liupan modify, 2017/8/1
    start_thread_time = int(time.time()) - time_now
    logger.info("start pid=%d time=%d" % (os.getpid(), start_thread_time))
    n_ip_equal_count = 0
    n_ip_count = 0
    n_user_count = 0
    # sum_band_width
    flu_sum = 0
    n_ip_pbs_count = 0
    dic_store_temp = {}
    dic_user_nodeip = {}
    ip_exist_dic = {}
    # cdn_detail=['none','kw','dl','ws','pbs','ctt'];

    # { liupan modify, 2017/8/1
    # if statis_version=='CNTV':
    #     cdn_detail=['qt','kw','dl','ws','pbs','ctt'];
    #     exist_cdn=['kw','dl','ws'];
    #     #bitrate_detail=[4000,2000,1500,850,500];
    #     bitrate_detail=read_bitrate_config();
    #     #cdn:channel:index:playTime
    # elif statis_version=='LD_KW':
    #     domain_channel=read_domain_channel_config();
    #     cdn_detail=['none','ld_kw','dl','ws','pbs','ctt'];
    #     bitrate_detail=read_ld_bitrate_config();
    # # { liupan add, 2017/7/11
    # elif statis_version == 'JAPAN':
    #     domain_channel = read_domain_channel_config();          # 需要配置域名对应频道名
    #     cdn_detail = ['none', 'sdzx'];
    #     bitrate_detail = read_ld_bitrate_config();
    # # } liupan add, 2017/7/11

    cdn_detail = cdn_all
    exist_cdn = agent_cdn
    global bitrate_detail
    global domain_channel
    bitrate_detail = read_bitrate_config_common()
    if domain_channel_flag == 1:
        domain_channel = read_domain_channel_config()
    # } liupan modify, 2017/8/1

    channel_bitrate_dic = {}
    # agent_count_dic={'all':{'agent_sum':0, 'agent_new_v1_sum':0, 'agent_new_v2_sum':0}};    # 2017/6/23 by liupan
    # { liupan modify, 2018/1/18
    # agent_count_dic={'all':{'agent_sum':0,'agent_new_sum':0}};
    # # 2017/6/23 by liupan
    agent_count_dic = {
        'all': {
            'agent_sum': 0,
            'agent_new_sum': 0,
            'edge_band_width-new': 0}}
    agent_channel_dic = {}
    # } liupan modify, 2018/1/18
    edge_data = {'band_width': 0, 'cdn': {}}
    # { liupan add, 2017/8/17
    sy_data = {'band_width': 0, 'cdn': {}}
    # } liupan add, 2017/8/17

    # for_test
    #result_list = get_test_data()

    for result in result_list:
        n_cdn_temp = result['from']
        str_node_ip = result['s_ip']
        ip_equal_dic = {}
        # { liupan delete, 2018/1/19
        # if not(str_node_ip in ip_dic):
        #     n_cdn_temp=0;
        #     #continue;
        # } liupan delete, 2018/1/19
        if str_node_ip in ip_equal_dic:
            n_ip_equal_count += 1
            continue
        else:
            ip_equal_dic[str_node_ip] = 1
        users_data_list = result['users']
        # { liupan modify, 2017/10/1
        # if n_cdn_temp != 6:
        if n_cdn_temp >= 0 and n_cdn_temp < len(cdn_all):
            # } liupan modify, 2017/10/1
            n_ip_count += 1
            for user_data in users_data_list:
                n_user_count += 1
                # { liupan modify, 2017/8/1
                # if statis_version=='CNTV':
                #     result=get_user_table_detail(user_data);
                # elif statis_version=='LD_KW':
                #     result=get_user_table_detail_ld(user_data,domain_channel);
                # # { liupan add, 2017/7/11
                # elif statis_version == 'JAPAN':
                #     result = get_user_table_detail_ld(user_data, domain_channel);       # ???
                # # } liupan add, 2017/7/11
                result = get_user_table_detail_common(user_data)
                # } liupan modify, 2017/8/1

                str_user_ip = result['u_ip']
                str_region = "其他"
                str_operator = "其他"
                flu_sum += result['flu'] * 8 / 1000 / 60

                # { liupan modify, 2017/8/1
                # # { liupan modify, 2017/7/11
                # if statis_version == 'CNTV' or statis_version == 'LD_KW':
                #     if str_user_ip in ip_exist_dic:
                #         _detail=ip_exist_dic[str_user_ip];
                #     else:
                #         _detail=get_ip_detail(str_user_ip,str_region,str_operator)
                #         ip_exist_dic[str_user_ip]=_detail;
                # elif statis_version == 'JAPAN':
                #     _detail = ['jp', 'sdzx'];
                # # } liupan modify, 2017/7/11
                if user_region_flag == 1:
                    if str_user_ip in ip_exist_dic:
                        _detail = ip_exist_dic[str_user_ip]
                    else:
                        _detail = get_ip_detail(
                            str_user_ip, str_region, str_operator)
                        ip_exist_dic[str_user_ip] = _detail
                else:
                    # { liupan modify, 2017/9/30
                    # _detail = [region_all[0], operator_all[0]];
                    _detail = ["其他", "其他"]
                    if str_node_ip in ip_dic:
                        _detail[0] = ip_dic[str_node_ip][2]
                        _detail[1] = ip_dic[str_node_ip][1]
                    # } liupan modify, 2017/9/30
                # } liupan modify, 2017/8/1

                str_region = _detail[0]
                str_operator = _detail[1]
                str_cdn = cdn_detail[n_cdn_temp]

                _node_detail = ["其他", "其他"]
                if str_node_ip in ip_dic:
                    _node_detail[0] = ip_dic[str_node_ip][2]
                    _node_detail[1] = ip_dic[str_node_ip][1]
                str_node_region = _node_detail[0]
                str_node_operator = _node_detail[1]

                if str_user_ip in ip_exist_dic:
                    _user_detail = ip_exist_dic[str_user_ip]
                else:
                    _user_detail = get_ip_detail(str_user_ip, str_region, str_operator)
                    ip_exist_dic[str_user_ip] = _user_detail
                str_user_region = _user_detail[0]
                str_user_operator = _user_detail[1]

                if str_node_ip in ip_dic:
                    level = int(ip_dic[str_node_ip][0])
                else:
                    level = 5
                if not(str_cdn in agent_count_dic):
                    # agent_count_dic[str_cdn]={'agent_sum':0, 'agent_new_v1_sum':0, 'agent_new_v2_sum':0};           # 2017/6/23 by liupan
                    # { liupan modify, 2018/1/18
                    # agent_count_dic[str_cdn]={'agent_sum':0,'agent_new_sum':0};
                    # # 2017/6/23 by liupan
                    agent_count_dic[str_cdn] = {
                        'agent_sum': 0, 'agent_new_sum': 0, 'edge_band_width-new': 0}
                    agent_channel_dic[str_cdn] = {}
                    # } liupan modify, 2018/1/18
                # { liupan modify, 2018/1/18
                # agent_temp=result['agent'];
                # am_temp=result['am'];
                am_temp = result['am']
                agent_temp = getAgentName(am_temp, result['agent'])
                # } liupan modify, 2018/1/18
                if level == 5 and agent_temp != 'none' and str_cdn in exist_cdn and result['suc_n'] != 0:
                    if not (agent_temp in agent_count_dic[str_cdn]):
                        agent_count_dic[str_cdn][agent_temp] = 0
                        # agent_count_dic[str_cdn][agent_temp + '-new-v1'] = 0;                       # 2017/6/23 by liupan
                        # agent_count_dic[str_cdn][agent_temp + '-new-v2'] = 0;
                        # # 2017/6/23 by liupan
                        # 2017/6/23 by liupan
                        agent_count_dic[str_cdn][agent_temp + '-new'] = 0
                        # agent_count_dic[str_cdn]['agent_sum']=0;
                        # agent_count_dic[str_cdn]['agent_new_sum']=0;

                        # { liupan add, 2018/1/18
                        agent_count_dic[str_cdn][agent_temp + '-bandwidth'] = 0
                        agent_count_dic[str_cdn][agent_temp +
                                                 '-bandwidth-new'] = 0
                        # } liupan add, 2018/1/18
                    if not (agent_temp in agent_count_dic['all']):
                        agent_count_dic['all'][agent_temp] = 0
                        # agent_count_dic['all'][agent_temp + '-new-v1'] = 0;                         # 2017/6/23 by liupan
                        # agent_count_dic['all'][agent_temp + '-new-v2'] = 0;
                        # # 2017/6/23 by liupan
                        # 2017/6/23 by liupan
                        agent_count_dic['all'][agent_temp + '-new'] = 0

                        # { liupan add, 2018/1/18
                        agent_count_dic['all'][agent_temp + '-bandwidth'] = 0
                        agent_count_dic['all'][agent_temp +
                                               '-bandwidth-new'] = 0
                        # } liupan add, 2018/1/18
                    agent_count_dic['all'][agent_temp] += 1
                    agent_count_dic['all']['agent_sum'] += 1
                    agent_count_dic[str_cdn][agent_temp] += 1
                    agent_count_dic[str_cdn]['agent_sum'] += 1

                    # { 2017/6/23 by liupan
                    # if 'hds' in agent_temp or am_temp % 4 == 1:
                    #     agent_count_dic['all']['agent_new_v1_sum'] += 1;
                    #     agent_count_dic['all'][agent_temp + '-new-v1'] += 1;
                    #     agent_count_dic[str_cdn]['agent_new_v1_sum'] += 1;
                    #     agent_count_dic[str_cdn][agent_temp + '-new-v1'] += 1;
                    # elif am_temp % 4 == 3:
                    #     agent_count_dic['all']['agent_new_v2_sum'] += 1;
                    #     agent_count_dic['all'][agent_temp + '-new-v2'] += 1;
                    #     agent_count_dic[str_cdn]['agent_new_v2_sum'] += 1;
                    #     agent_count_dic[str_cdn][agent_temp + '-new-v2'] += 1;

                    # { liupan modify, 2018/1/18
                    # if am_temp%2==1 or 'hds' in agent_temp:
                    if isNewAgent(am_temp, agent_temp):
                        # } liupan modify, 2018/1/18
                        agent_count_dic['all']['agent_new_sum'] += 1
                        agent_count_dic['all'][agent_temp + '-new'] += 1
                        agent_count_dic[str_cdn]['agent_new_sum'] += 1
                        agent_count_dic[str_cdn][agent_temp + '-new'] += 1
                    # } 2017/6/23 by liupan
                vip_flag = 0
                if str_node_ip in vip_list:
                    vip_flag = 1
                # channel statistic
                if not(str_cdn in channel_bitrate_dic):
                    channel_bitrate_dic[str_cdn] = {}
                for channel_name, ch_bit in result["channel_bit"].items():
                    if not(channel_name in channel_bitrate_dic[str_cdn]):
                        channel_bitrate_dic[str_cdn][channel_name] = {}
                        # { liupan add, 2017/8/17
                        channel_bitrate_dic[str_cdn][channel_name + '-sy'] = {}
                        # } liupan add, 2017/8/17

                        # { liupan modify, 2017/8/1
                        # if statis_version=='CNTV':
                        #     channel_bitrate_dic[str_cdn][channel_name]={'0':0,'1':0,'2':0,'3':0,'4':0};
                        # elif statis_version=='LD_KW':
                        #     for bitrate_index in bitrate_detail:
                        #         channel_bitrate_dic[str_cdn][channel_name][bitrate_index]=0;
                        # # { liupan add, 2017/7/11
                        # elif statis_version == 'JAPAN':
                        #     for bitrate_index in bitrate_detail:
                        #         channel_bitrate_dic[str_cdn][channel_name][bitrate_index] = 0;
                        # # } liupan add, 2017/7/11
                        for bitrate_index in bitrate_detail:        # 配置文件中的码率类型名称应和数据库中存储的名称相一致
                            channel_bitrate_dic[str_cdn][channel_name][bitrate_index] = 0
                            # { liupan add, 2017/8/17
                            channel_bitrate_dic[str_cdn][channel_name +
                                                         '-sy'][bitrate_index] = 0
                            # } liupan add, 2017/8/17
                        # } liupan modify, 2017/8/1
                        # print(channel_bitrate_dic);
                    for index_bit, n_bit in ch_bit.items():
                        # { liupan add, 2017/9/1
                        if index_bit not in bitrate_detail:
                            # continue;#lp
                            bitrate_detail[index_bit] = int(index_bit)
                            channel_bitrate_dic[str_cdn][channel_name][index_bit] = 0
                            channel_bitrate_dic[str_cdn][channel_name +
                                                         '-sy'][index_bit] = 0
                        # } liupan add, 2017/9/1
                        channel_bitrate_dic[str_cdn][channel_name][index_bit] += n_bit
                        # { liupan add, 2017/8/17
                        if result['sy'] == 1:
                            channel_bitrate_dic[str_cdn][channel_name +
                                                         '-sy'][index_bit] += n_bit
                        # } liupan add, 2017/8/17
                # continue;
                for channel_name, channel_flu in result["channel_flu"].items():

                    band_width_temp = int(channel_flu) * 8 / 1000 / 60
                    if str_node_region not in dic_user_nodeip:
                        dic_user_nodeip[str_node_region] = {}
                    if str_cdn not in dic_user_nodeip[str_node_region]:
                        dic_user_nodeip[str_node_region][str_cdn] = {}
                    if str_node_operator not in dic_user_nodeip[str_node_region][str_cdn]:
                        dic_user_nodeip[str_node_region][str_cdn][str_node_operator] = {}
                    if str_node_operator not in dic_user_nodeip[str_node_region][str_cdn][str_node_operator]:
                        dic_user_nodeip[str_node_region][str_cdn][str_node_operator][vip_flag] = {}
                    if str_node_ip not in dic_user_nodeip[str_node_region][str_cdn][str_node_operator][vip_flag]:
                        dic_user_nodeip[str_node_region][str_cdn][str_node_operator][vip_flag][str_node_ip] = {}
                    if str_user_region not in dic_user_nodeip[str_node_region][str_cdn][str_node_operator][vip_flag][str_node_ip]:
                        dic_user_nodeip[str_node_region][str_cdn][str_node_operator][vip_flag][str_node_ip][str_user_region] = {}
                    if str_user_operator not in dic_user_nodeip[str_node_region][str_cdn][str_node_operator][vip_flag][str_node_ip][str_user_region]:
                        dic_user_nodeip[str_node_region][str_cdn][str_node_operator][vip_flag][str_node_ip][str_user_region][str_user_operator] = {}
                    if channel_name not in dic_user_nodeip[str_node_region][str_cdn][str_node_operator][vip_flag][str_node_ip][str_user_region][str_user_operator]:
                        dic_user_nodeip[str_node_region][str_cdn][str_node_operator][vip_flag][str_node_ip][str_user_region][
                            str_user_operator][channel_name] = {
                            'band_width': 0,
                            'freeze_count': 0,
                            'level': level,
                            'vip': vip_flag,
                            'request_count': 0,
                            'success_count': 0,
                            'all_count': 0,
                            'bitrate_count': 0,
                            'bitrate_sum': 0,
                            'duration': 0,
                            'jam_all': 0,
                            'delayed_avg': 0,
                            'delayed_avg_n': 0
                        }
                    if 'delayed_avg' in result and result['delayed_avg'] != 0:
                        dic_user_nodeip[str_node_region][str_cdn][str_node_operator][vip_flag][str_node_ip][str_user_region][
                            str_user_operator][channel_name]['delayed_avg_n'] += 1
                        dic_user_nodeip[str_node_region][str_cdn][str_node_operator][vip_flag][str_node_ip][str_user_region][
                            str_user_operator][channel_name]['delayed_avg'] += result['delayed_avg']
                    dic_user_nodeip[str_node_region][str_cdn][str_node_operator][vip_flag][str_node_ip][str_user_region][
                        str_user_operator][channel_name]['band_width'] += band_width_temp
                    dic_user_nodeip[str_node_region][str_cdn][str_node_operator][vip_flag][str_node_ip][str_user_region][
                        str_user_operator][channel_name]['request_count'] += result['req_n']
                    dic_user_nodeip[str_node_region][str_cdn][str_node_operator][vip_flag][str_node_ip][str_user_region][
                        str_user_operator][channel_name]['success_count'] += result['suc_n']
                    dic_user_nodeip[str_node_region][str_cdn][str_node_operator][vip_flag][str_node_ip][str_user_region][
                        str_user_operator][channel_name]['duration'] += result['duration']
                    dic_user_nodeip[str_node_region][str_cdn][str_node_operator][vip_flag][str_node_ip][str_user_region][
                        str_user_operator][channel_name]['jam_all'] += result['jam_all']


                    if str_region not in dic_store_temp:
                        dic_store_temp[str_region] = {}
                    if str_cdn not in dic_store_temp[str_region]:
                        dic_store_temp[str_region][str_cdn] = {}
                    if str_operator not in dic_store_temp[str_region][str_cdn]:
                        dic_store_temp[str_region][str_cdn][str_operator] = {}
                    if vip_flag not in dic_store_temp[str_region][str_cdn][str_operator]:
                        dic_store_temp[str_region][str_cdn][str_operator][vip_flag] = {
                        }
                    # 初始化 dic_store_temp
                    if channel_name not in dic_store_temp[str_region][str_cdn][str_operator][vip_flag]:
                        # { liupan modify, 2018/5/30
                        # # { liupan modify, 2017/8/17
                        # # dic_store_temp[str_region][str_cdn][str_operator][vip_flag][channel_name]={'band_width':0,'freeze_count':0,'level':level,'vip':vip_flag,'request_count':0,'success_count':0,'all_count':1,'bitrate_count':0,'bitrate_sum':0,'node':{}};
                        # dic_store_temp[str_region][str_cdn][str_operator][vip_flag][channel_name]={'band_width':0,'sy_band_width':0,'freeze_count':0,'level':level,'vip':vip_flag,'request_count':0,'success_count':0,'all_count':1,'bitrate_count':0,'bitrate_sum':0,'node':{}};
                        # # } liupan modify, 2017/8/17
                        dic_store_temp[str_region][str_cdn][str_operator][vip_flag][channel_name] = {
                            'band_width': 0,
                            'sy_band_width': 0,
                            'freeze_count': 0,
                            'level': level,
                            'vip': vip_flag,
                            'request_count': 0,
                            'success_count': 0,
                            'all_count': 0,
                            'bitrate_count': 0,
                            'bitrate_sum': 0,
                            'duration': 0,
                            'jam_all': 0,
                            'ps_freeze_count': 0,
                            'delayed_avg': 0,
                            'delayed_avg_n': 0,
                            'node': {}}
                        # } liupan modify, 2018/5/30
                        # { liupan add, 2018/6/14
                        # dic_store_temp[str_region][str_cdn][str_operator][vip_flag][channel_name]['ps_freeze_count'] = 0
                        # # } liupan add, 2018/6/14
                        #
                        # # add by qjk= 0
                        dic_store_temp[str_region][str_cdn][str_operator][vip_flag][channel_name]['freeze_avg_iv'] = 0
                        dic_store_temp[str_region][str_cdn][str_operator][vip_flag][channel_name]['freeze_avg_n'] = 0

                    # add by qjk
                    if 'freeze_avg_iv' in result and result['freeze_avg_iv'] != 0:
                        dic_store_temp[str_region][str_cdn][str_operator][vip_flag][channel_name]['freeze_avg_n'] += 1
                        dic_store_temp[str_region][str_cdn][str_operator][vip_flag][
                            channel_name]['freeze_avg_iv'] += result['freeze_avg_iv']
                    if 'delayed_avg' in result and result['delayed_avg'] != 0:
                        dic_store_temp[str_region][str_cdn][str_operator][vip_flag][channel_name]['delayed_avg_n'] += 1
                        dic_store_temp[str_region][str_cdn][str_operator][vip_flag][channel_name]['delayed_avg'] += result['delayed_avg']

                    if str_node_ip not in dic_store_temp[str_region][str_cdn][
                            str_operator][vip_flag][channel_name]['node']:
                        # { liupan modify, 2018/5/30
                        # dic_store_temp[str_region][str_cdn][str_operator][vip_flag][channel_name]['node'][str_node_ip]={'band_width':0,'freeze_count':0,'request_count':0,'success_count':0,'all_count':1,'bitrate_count':0,'bitrate_sum':0};
                        dic_store_temp[str_region][str_cdn][str_operator][vip_flag][channel_name]['node'][str_node_ip] = {
                            'band_width': 0,
                            'freeze_count': 0,
                            'request_count': 0,
                            'success_count': 0,
                            'all_count': 0,
                            'bitrate_count': 0,
                            'bitrate_sum': 0,
                            'duration': 0,
                            'jam_all': 0
                        }
                        # } liupan modify, 2018/5/30

                    dic_store_temp[str_region][str_cdn][str_operator][vip_flag][channel_name]['request_count'] += result['req_n']
                    dic_store_temp[str_region][str_cdn][str_operator][vip_flag][channel_name]['success_count'] += result['suc_n']
                    dic_store_temp[str_region][str_cdn][str_operator][vip_flag][channel_name]['duration'] += result['duration']
                    dic_store_temp[str_region][str_cdn][str_operator][vip_flag][channel_name]['jam_all'] += result['jam_all']
                    # band_width_temp=result['flu']*8/1000/60;
                    # band_width_temp = int(channel_flu) * 8 / 1000 / 60
                    dic_store_temp[str_region][str_cdn][str_operator][vip_flag][channel_name]['band_width'] += band_width_temp
                    # dic_store_temp[str_region][str_cdn][str_operator]['node'][str_node_ip]={'band_width':0,'freeze_count':0,'request_count':0,'success_count':0,'all_count':1,'bitrate_count':0,'bitrate_sum':0};
                    dic_store_temp[str_region][str_cdn][str_operator][vip_flag][
                        channel_name]['node'][str_node_ip]['request_count'] += result['req_n']
                    dic_store_temp[str_region][str_cdn][str_operator][vip_flag][
                        channel_name]['node'][str_node_ip]['success_count'] += result['suc_n']
                    dic_store_temp[str_region][str_cdn][str_operator][vip_flag][
                        channel_name]['node'][str_node_ip]['band_width'] += band_width_temp

                    dic_store_temp[str_region][str_cdn][str_operator][vip_flag][
                        channel_name]['node'][str_node_ip]['duration'] += result['duration']
                    dic_store_temp[str_region][str_cdn][str_operator][vip_flag][
                        channel_name]['node'][str_node_ip]['jam_all'] += result['jam_all']
                    if (level != 3 and level != 4) or vip_flag:
                        # { liupan modify, 2018/5/31
                        # dic_store_temp[str_region][str_cdn][str_operator][vip_flag][channel_name]['all_count']+=1;
                        # dic_store_temp[str_region][str_cdn][str_operator][vip_flag][channel_name]['node'][str_node_ip]['all_count']+=1;
                        dic_store_temp[str_region][str_cdn][str_operator][vip_flag][channel_name]['all_count'] += result['req_n']
                        dic_store_temp[str_region][str_cdn][str_operator][vip_flag][
                            channel_name]['node'][str_node_ip]['all_count'] += result['req_n']
                        # } liupan modify, 2018/5/31
                        bitrate_temp = 0
                        bitrate_time = 0
                        # for key,value in result['rate_n'].items():
                        for key, value in result["channel_bit"][channel_name].items():
                            # { liupan modify, 2017/8/1
                            # if statis_version=='CNTV':
                            #     bitrate_temp+=bitrate_detail[int(key)]*value;
                            # elif statis_version=='LD_KW':
                            #     bitrate_temp+=int(key)*value
                            # # { liupan add, 2017/7/11
                            # elif statis_version == 'JAPAN':
                            #     bitrate_temp += int(key) * value;
                            # # } liupan add, 2017/7/11
                            if key not in bitrate_detail:   # liupan modify, 2017/10/31
                                bitrate_temp += int(key) * value
                            else:
                                bitrate_temp += bitrate_detail[key] * value
                            # } liupan modify, 2017/8/1
                            bitrate_time += value
                        if bitrate_time != 0:
                            dic_store_temp[str_region][str_cdn][str_operator][vip_flag][channel_name]['bitrate_sum'] += bitrate_temp
                            dic_user_nodeip[str_node_region][str_cdn][str_node_operator][vip_flag][str_node_ip][str_user_region][
                                str_user_operator][channel_name]['bitrate_sum'] += bitrate_temp
                            dic_store_temp[str_region][str_cdn][str_operator][vip_flag][channel_name]['bitrate_count'] += bitrate_time
                            dic_user_nodeip[str_node_region][str_cdn][str_node_operator][vip_flag][str_node_ip][str_user_region][
                                str_user_operator][channel_name]['bitrate_count'] += bitrate_time
                            dic_store_temp[str_region][str_cdn][str_operator][vip_flag][
                                channel_name]['node'][str_node_ip]['bitrate_sum'] += (bitrate_temp)
                            dic_store_temp[str_region][str_cdn][str_operator][vip_flag][
                                channel_name]['node'][str_node_ip]['bitrate_count'] += bitrate_time
                        if result['jam']:
                            # { liupan modify, 2018/5/31
                            # dic_store_temp[str_region][str_cdn][str_operator][vip_flag][channel_name]['freeze_count']+=1;
                            # dic_store_temp[str_region][str_cdn][str_operator][vip_flag][channel_name]['node'][str_node_ip]['freeze_count']+=1;
                            dic_store_temp[str_region][str_cdn][str_operator][vip_flag][channel_name]['freeze_count'] += result['jam']
                            dic_user_nodeip[str_node_region][str_cdn][str_node_operator][vip_flag][str_node_ip][str_user_region][
                                str_user_operator][channel_name]['freeze_count'] += result['jam']
                            dic_store_temp[str_region][str_cdn][str_operator][vip_flag][
                                channel_name]['node'][str_node_ip]['freeze_count'] += result['jam']
                            # } liupan modify, 2018/5/31
                        if str_cdn not in edge_data['cdn']:
                            edge_data['cdn'][str_cdn] = {'band_width': 0}
                        edge_data['band_width'] += band_width_temp
                        edge_data['cdn'][str_cdn]['band_width'] += band_width_temp
                        # { liupan add, 2018/1/18
                        # if agent_temp != 'none':
                        if level == 5 and agent_temp != 'none' and str_cdn in exist_cdn and result['suc_n'] != 0:
                            if not(channel_name in agent_channel_dic[str_cdn]):
                                agent_channel_dic[str_cdn][channel_name] = {
                                    'bandwidth-new': 0}
                            if not (agent_temp + "-bandwidth") in agent_channel_dic[str_cdn][channel_name]:
                                agent_channel_dic[str_cdn][channel_name][agent_temp +
                                                                         '-bandwidth'] = 0
                                agent_channel_dic[str_cdn][channel_name][agent_temp +
                                                                         '-bandwidth-new'] = 0

                            agent_count_dic['all'][agent_temp +
                                                   '-bandwidth'] += band_width_temp
                            agent_count_dic[str_cdn][agent_temp +
                                                     '-bandwidth'] += band_width_temp
                            agent_channel_dic[str_cdn][channel_name][agent_temp +
                                                                     '-bandwidth'] += band_width_temp

                            if isNewAgent(am_temp, agent_temp):
                                agent_count_dic['all'][agent_temp +
                                                       '-bandwidth-new'] += band_width_temp
                                agent_count_dic['all']['edge_band_width-new'] += band_width_temp
                                agent_count_dic[str_cdn][agent_temp +
                                                         '-bandwidth-new'] += band_width_temp
                                agent_count_dic[str_cdn]['edge_band_width-new'] += band_width_temp
                                agent_channel_dic[str_cdn][channel_name][agent_temp +
                                                                         '-bandwidth-new'] += band_width_temp
                                agent_channel_dic[str_cdn][channel_name]['bandwidth-new'] += band_width_temp
                        # } liupan add, 2018/1/18

                        # { liupan add, 2017/8/17
                        if str_cdn not in sy_data['cdn']:
                            sy_data['cdn'][str_cdn] = {'band_width': 0}
                        if result['sy'] == 1:
                            sy_data['band_width'] += band_width_temp
                            sy_data['cdn'][str_cdn]['band_width'] += band_width_temp
                        # } liupan add, 2017/8/17

                    # { liupan add, 2017/8/17
                    if result['sy'] == 1:
                        dic_store_temp[str_region][str_cdn][str_operator][vip_flag][channel_name]['sy_band_width'] += band_width_temp
                    # } liupan add, 2017/8/17
                # { liupan add, 2018/6/12
                if 'user_n' not in dic_store_temp[str_region][str_cdn][str_operator][vip_flag]:
                    dic_store_temp[str_region][str_cdn][str_operator][vip_flag]['user_n'] = 0
                    dic_store_temp[str_region][str_cdn][str_operator][vip_flag]['req_n'] = 0
                dic_store_temp[str_region][str_cdn][str_operator][vip_flag]['user_n'] += 1
                dic_store_temp[str_region][str_cdn][str_operator][vip_flag]['req_n'] += result['req_n']

                if 'user_n' not in dic_user_nodeip[str_node_region][str_cdn][str_node_operator][vip_flag][str_node_ip][str_user_region][
                        str_user_operator]:
                    dic_user_nodeip[str_node_region][str_cdn][str_node_operator][vip_flag][str_node_ip][
                        str_user_region][str_user_operator]['user_n'] = 0
                    dic_user_nodeip[str_node_region][str_cdn][str_node_operator][vip_flag][str_node_ip][
                        str_user_region][str_user_operator]['req_n'] = 0

                dic_user_nodeip[str_node_region][str_cdn][str_node_operator][vip_flag][str_node_ip][
                    str_user_region][str_user_operator]['user_n'] += 1
                dic_user_nodeip[str_node_region][str_cdn][str_node_operator][vip_flag][str_node_ip][
                    str_user_region][str_user_operator]['req_n'] += result['req_n']

                # } liupan add, 2018/6/12
                # { liupan add, 2018/6/14
                if push_stream_stability_flag:
                    ps_freeze_count_tmp = {}
                    all_freeze_flag = True
                    channel_count_tmp = 0
                    channel_group_tmp = ""
                    for channel_name in result['channelnames']:
                        channel_group = result['channelnames'][channel_name]
                        if channel_group not in ps_freeze_count_tmp:
                            ps_freeze_count_tmp[channel_group] = 0
                            channel_count_tmp += 1
                            channel_group_tmp = channel_group

                        isChannelPushFreezeFlag = False
                        if channel_name in channel_freeze_dict:
                            isChannelPushFreezeFlag = channel_freeze_dict[channel_name]
                        else:
                            if result['jam'] > 0:
                                isChannelPushFreezeFlag = isChannelPushFreeze(
                                    channel_name, start)
                                channel_freeze_dict[channel_name] = isChannelPushFreezeFlag

                        if isChannelPushFreezeFlag:
                            ps_freeze_count_tmp[channel_group] += 1
                        else:
                            all_freeze_flag = False
                    if all_freeze_flag and channel_count_tmp == 1:
                        dic_store_temp[str_region][str_cdn][str_operator][vip_flag][channel_group_tmp]['ps_freeze_count'] += result['jam']
                    else:
                        for channel_group in ps_freeze_count_tmp:
                            dic_store_temp[str_region][str_cdn][str_operator][vip_flag][
                                channel_group]['ps_freeze_count'] += ps_freeze_count_tmp[channel_group]
                # } liupan add, 2018/6/14
        else:
            n_ip_pbs_count += 1
            continue

    # print(channel_bitrate_dic);
    # { liupan delete, 2017/8/1
    # 为了确保在往p_queue中插入数据时，其它的数据已经统计完成，需要将该语句移到函数末尾执行。
    # p_queue.put(channel_bitrate_dic);
    # } liupan delete, 2017/8/1
    # print(agent_count_dic);
    # 初始化his_cdn_data（history_user_sum 中的cdn）  his_out_data（history_user_sum） his_channel_data（user_channel_cdn_single）


    write_db = {"user_statistics": [], "time": start}
    dic_store_flu_sum = 0
    his_cdn_data = {}
    his_cdn_count = {}
    his_channel_data = {}
    his_channel_count = {}
    his_count = {
        'freeze_rate': 0,
        'bitrate': 0,
        'success_rate': 0,
        'delayed_avg_n': 0
    }
    his_out_data = {
        'freeze_rate': 0,
        'bitrate': 0,
        'success_rate': 0,
        'band_width': 0,
        'user_n': 0,
        'req_n': 0,
        'duration': 0,
        'jam_all': 0,
        'ps_freeze_rate': 0,
        'delayed_avg': 0
    }

    his_count['freeze_avg_n'] = 0
    his_out_data['freeze_avg_iv'] = 0

    time_temp = int(time.time()) - time_now

    # { liupan delete, 2017/8/1
    # # { liupan modify, 2017/7/13
    # #if statis_version=='CNTV':
    # #    main_client=MongoClient("192.168.2.30", 27017)
    # #    main_db=main_client.cntv_log_db
    # #elif statis_version=='LD_KW':
    # #    main_client=MongoClient("10.27.219.220", 27017)
    # #    main_db=main_client.ld_log_db
    # ## { liupan add, 2017/7/11
    # #elif statis_version == 'JAPAN':
    # #    main_client = MongoClient("10.27.219.220", 27017);      # ???
    # #    main_db = main_client.jp_log_db;
    # ## } liupan add, 2017/7/11
    # main_client = MongoClient(mongodb_main_ip, mongodb_main_port);
    # main_db = main_client.get_database(mongodb_db);
    # # } liupan modify, 2017/7/13
    #
    # user_in=main_db.statistic_user
    # history_in=main_db.history_user_single;
    # history_sum_in=main_db.history_user_sum;
    # channel_sum_in=main_db.user_channel_cdn_single;
    # } liupan delete, 2017/8/1
    for node_region, cdn_dic in dic_user_nodeip.items():
        for cdn, node_operator_dic in cdn_dic.items():
            for node_operator, vip_dic in node_operator_dic.items():
                for vip_flags, node_ip_dic in vip_dic.items():
                    for node_ip, user_region_dic in node_ip_dic.items():
                        for user_region, user_operator_dic in user_region_dic.items():
                            for user_operator, channel_dic in user_operator_dic.items():
                                _band_width = 0
                                _duration = 0
                                _jam_all = 0
                                s_count, s_sum = 0, 0
                                f_count, f_sum = 0, 0
                                b_count, b_sum = 0, 0
                                for channel, detail_dic in channel_dic.items():
                                    if channel == 'user_n' or channel == 'req_n':
                                        continue
                                    s_count += detail_dic['success_count']
                                    s_sum += detail_dic['request_count']
                                    f_count += detail_dic['freeze_count']
                                    f_sum += detail_dic['all_count']
                                    b_count += detail_dic['bitrate_sum']
                                    b_sum += detail_dic['bitrate_count']
                                    _level = detail_dic['level']
                                    _band_width += detail_dic['band_width']
                                    _duration += detail_dic['duration']
                                    _jam_all += detail_dic['jam_all']
                                history_user_by_node_dict = {
                                    "time": start,
                                    "cdn": cdn,
                                    "s_region": node_region,
                                    "s_ip": node_ip,
                                    "s_operator": node_operator,
                                    'u_region': user_region,
                                    'u_operator': user_operator,
                                    "freeze_rate": f_count,
                                    "freeze_rate_sum": f_sum,
                                    "success_rate": s_count,
                                    "success_rate_sum": s_sum,
                                    "band_width": _band_width,
                                    "level": _level,
                                    "vip": vip_flags,
                                    'bit_sum': b_count,
                                    'bit_time': b_sum,
                                    'duration': _duration,
                                    'jam_all': _jam_all,
                                    'user_n': channel_dic['user_n'],
                                    'req_n': channel_dic['req_n']
                                }
                                #logger.info(history_user_by_node_dict)
                                queue_history_user_by_node.put(history_user_by_node_dict)
                                history_user_by_node_count[count_index] += 1



# dic_store_temp {区域：{cdn:{运营商：{0(非vip)：{channel_name:{,node{127.0.0.1:{}.
    for region, cdn_dic in dic_store_temp.items():
        for cdn, operator_dic in cdn_dic.items():
            if not(cdn in his_cdn_data):
                his_cdn_data[cdn] = {
                    'freeze_rate': 0,
                    'bitrate': 0,
                    'success_rate': 0,
                    'band_width': 0,
                    'user_n': 0,
                    'req_n': 0,
                    'duration': 0,
                    'jam_all': 0,
                    'ps_freeze_rate': 0,
                    'delayed_avg': 0
                }
                his_cdn_count[cdn] = {
                    'freeze_rate': 0,
                    'bitrate': 0,
                    'success_rate': 0,
                    'delayed_avg_n': 0
                }

                his_cdn_count[cdn]['freeze_avg_n'] = 0
                his_cdn_data[cdn]['freeze_avg_iv'] = 0
                # { liupan add, 2018/6/12
                # his_cdn_data[cdn]['user_n'] = 0
                # his_cdn_data[cdn]['req_n'] = 0
                # his_cdn_data[cdn]['duration'] = 0
                # his_cdn_data[cdn]['jam_all'] = 0
                # # } liupan add, 2018/6/12
                # # { liupan add, 2018/6/14
                # his_cdn_data[cdn]['ps_freeze_rate'] = 0
                # # } liupan add, 2018/6/14
                #
                # # add by qjk
                # his_cdn_data[cdn]['delayed_avg'] = 0
                # his_cdn_count[cdn]['delayed_avg_n'] = 0
                # his_cdn_count[cdn]['freeze_avg_n'] = 0
                # his_cdn_data[cdn]['freeze_avg_iv'] = 0

            if not(cdn in his_channel_data):
                his_channel_data[cdn] = {}
                his_channel_count[cdn] = {}
            for operator, vip_dic in operator_dic.items():
                for vip_flags, channel_dic in vip_dic.items():
                    _band_width = 0
                    # { liupan add, 2017/8/17
                    _sy_band_width = 0
                    # xuzj add 2018-08-09
                    _duration = 0
                    _jam_all = 0
                    # } liupan add, 2017/8/17
                    s_count, s_sum = 0, 0
                    f_count, f_sum = 0, 0
                    # { liupan add, 2018/6/14
                    pf_count = 0
                    # } liupan add, 2018/6/14
                    b_count, b_sum = 0, 0
                    node_ip_detail = []
                    # 累加各频道平均卡顿间隔
                    f_agv_map = {
                        'f_avg': 0,
                        'f_avg_n': 0
                    }
                    # 累加各频道平均延时
                    d_agv_map = {
                        'd_avg': 0,
                        'd_avg_n': 0
                    }

                    for channel, detail_dic in channel_dic.items():
                        # { liupan add, 2018/6/12
                        if channel == 'user_n' or channel == 'req_n':
                            his_cdn_data[cdn][channel] += detail_dic
                            his_out_data[channel] += detail_dic
                            continue
                        # } liupan add, 2018/6/12
                        if not(channel in his_channel_data[cdn]):
                            # { liupan modify, 2017/8/17
                            # his_channel_data[cdn][channel]={'freeze_rate':0,'bitrate':0,'success_rate':0,'band_width':0}
                            his_channel_data[cdn][channel] = {
                                'freeze_rate': 0,
                                'bitrate': 0,
                                'success_rate': 0,
                                'band_width': 0,
                                'sy_band_width': 0,
                                'duration': 0,
                                'jam_all': 0,
                                'ps_freeze_rate': 0,
                                'delayed_avg': 0

                            }
                            # } liupan modify, 2017/8/17
                            his_channel_count[cdn][channel] = {
                                'freeze_rate': 0,
                                'bitrate': 0,
                                'success_rate': 0,
                                'delayed_avg_n': 0
                            }
                            # { liupan add, 2018/6/14
                            # his_channel_data[cdn][channel]['ps_freeze_rate'] = 0
                            # # } liupan add, 2018/6/14
                            #
                            # # add by qjk
                            his_channel_data[cdn][channel]['freeze_avg_iv'] = 0
                            his_channel_count[cdn][channel]['freeze_avg_n'] = 0
                            # his_channel_data[cdn][channel]['delayed_avg'] = 0
                            # his_channel_count[cdn][channel]['delayed_avg_n'] = 0

                        # { liupan modify, 2018/6/1
                        # s_count+=(detail_dic['success_count']*100);
                        s_count += detail_dic['success_count']
                        # } liupan modify, 2018/6/1
                        s_sum += detail_dic['request_count']
                        f_count += detail_dic['freeze_count']
                        # { liupan add, 2018/6/14
                        pf_count += detail_dic['ps_freeze_count']
                        # } liupan add, 2018/6/14
                        f_sum += detail_dic['all_count']
                        b_count += detail_dic['bitrate_sum']
                        b_sum += detail_dic['bitrate_count']

                        # add by qjk
                        his_channel_data[cdn][channel]['freeze_avg_iv'] += detail_dic['freeze_avg_iv']
                        his_channel_count[cdn][channel]['freeze_avg_n'] += detail_dic['freeze_avg_n']
                        his_channel_data[cdn][channel]['delayed_avg'] += detail_dic['delayed_avg']
                        his_channel_count[cdn][channel]['delayed_avg_n'] += detail_dic['delayed_avg_n']
                        if detail_dic['freeze_avg_iv'] > 0:
                            f_agv_map['f_avg'] += detail_dic['freeze_avg_iv']
                            f_agv_map['f_avg_n'] += 1
                        if detail_dic['delayed_avg'] > 0:
                            d_agv_map['d_avg'] += detail_dic['delayed_avg']
                            d_agv_map['d_avg_n'] += 1

                        # channel statistics.
                        # { liupan modify, 2018/6/1
                        # if detail_dic['request_count']!=0:
                        #     his_channel_data[cdn][channel]['success_rate']+=detail_dic['success_count']*100/detail_dic['request_count'];
                        #     his_channel_count[cdn][channel]['success_rate']+=1;
                        his_channel_data[cdn][channel]['success_rate'] += detail_dic['success_count']
                        his_channel_count[cdn][channel]['success_rate'] += detail_dic['request_count']
                        # } liupan modify, 2018/6/1
                        # { liupan modify, 2018/5/31
                        # if detail_dic['all_count']!=0:
                        #     his_channel_data[cdn][channel]['freeze_rate']+=detail_dic['freeze_count']*100/detail_dic['all_count'];
                        #     his_channel_count[cdn][channel]['freeze_rate']+=1;
                        his_channel_data[cdn][channel]['freeze_rate'] += detail_dic['freeze_count']
                        his_channel_count[cdn][channel]['freeze_rate'] += detail_dic['all_count']
                        # } liupan modify, 2018/5/31
                        # { liupan add, 2018/6/14
                        his_channel_data[cdn][channel]['ps_freeze_rate'] += detail_dic['ps_freeze_count']
                        his_channel_data[cdn][channel]['duration'] += detail_dic['duration']
                        his_channel_data[cdn][channel]['jam_all'] += detail_dic['jam_all']
                        # } liupan add, 2018/6/14
                        if detail_dic['bitrate_count'] != 0:
                            his_channel_data[cdn][channel]['bitrate'] += detail_dic['bitrate_sum']
                            his_channel_count[cdn][channel]['bitrate'] += detail_dic['bitrate_count']
                        his_channel_data[cdn][channel]['band_width'] += detail_dic['band_width']
                        # { liupan add, 2017/8/17
                        his_channel_data[cdn][channel]['sy_band_width'] += detail_dic['sy_band_width']
                        # } liupan add, 2017/8/17

                        _level = detail_dic['level']
                        _vip = detail_dic['vip']
                        _band_width += detail_dic['band_width']
                        # { liupan add, 2017/8/17
                        _sy_band_width += detail_dic['sy_band_width']
                        _duration += detail_dic['duration']
                        _jam_all += detail_dic['jam_all']

                        # } liupan add, 2017/8/17
                        his_out_data['band_width'] += detail_dic['band_width']
                        his_cdn_data[cdn]['band_width'] += detail_dic['band_width']
                        his_out_data['duration'] += detail_dic['duration']
                        his_cdn_data[cdn]['duration'] += detail_dic['duration']
                        his_out_data['jam_all'] += detail_dic['jam_all']
                        his_cdn_data[cdn]['jam_all'] += detail_dic['jam_all']
                        dic_store_flu_sum += detail_dic['band_width']
                        for node_ip, node_detail_store in detail_dic['node'].items():
                            _band_width2 = 0
                            #xuzj add 2018-08-09
                            _duration2 = 0
                            _jam_all2 = 0
                            s_count2, s_sum2 = 0, 0
                            f_count2, f_sum2 = 0, 0
                            b_count2, b_sum2 = 0, 0
                            # { liupan modify, 2018/6/1
                            # s_count2+=(node_detail_store['success_count']*100);
                            s_count2 += node_detail_store['success_count']
                            # } liupan modify, 2018/6/1
                            s_sum2 += node_detail_store['request_count']
                            f_count2 += node_detail_store['freeze_count']
                            f_sum2 += node_detail_store['all_count']
                            b_count2 += node_detail_store['bitrate_sum']
                            b_sum2 += node_detail_store['bitrate_count']
                            _band_width2 += node_detail_store['band_width']
                            _duration2 += node_detail_store['duration']
                            _jam_all2 += node_detail_store['jam_all']
                            _success_rate2 = 0
                            _freeze_rate2 = 0
                            _bitrate2 = 0
                            if s_sum2 != 0:
                                # { liupan modify, 2018/6/1
                                # _success_rate2=s_count2/s_sum2;
                                _success_rate2 = s_count2 * 100 / s_sum2
                                # } liupan modify, 2018/6/1
                            if f_sum2 != 0:
                                _freeze_rate2 = f_count2 * 100 / f_sum2
                            if b_sum2 != 0:
                                _bitrate2 = b_count2 / b_sum2
                            ip_info = {
                                "freeze_rate": _freeze_rate2,
                                "success_rate": _success_rate2,
                                "bitrate": _bitrate2,
                                "band_width": _band_width2,
                                "level": _level,
                                "ip": node_ip,
                                'bit_sum': node_detail_store['bitrate_sum'],
                                'bit_time': node_detail_store['bitrate_count'],
                                'duration': _duration2,
                                'jam_all': _jam_all2
                            }
                            node_ip_detail.append(ip_info)

                    _success_rate = 0
                    _freeze_rate = 0
                    _bitrate = 0
                    if s_sum != 0:
                        # { liupan modify, 2018/6/1
                        # _success_rate=s_count/s_sum;
                        # his_count['success_rate']+=1;
                        # his_out_data['success_rate']+=_success_rate;
                        # his_cdn_data[cdn]['success_rate']+=_success_rate;
                        # his_cdn_count[cdn]['success_rate']+=1;
                        _success_rate = s_count * 100 / s_sum
                        his_count['success_rate'] += s_sum
                        his_out_data['success_rate'] += s_count
                        his_cdn_data[cdn]['success_rate'] += s_count
                        his_cdn_count[cdn]['success_rate'] += s_sum
                        # } liupan modify, 2018/6/1
                    if f_sum != 0:
                        _freeze_rate = f_count * 100 / f_sum
                        # { liupan modify, 2018/5/31
                        # his_count['freeze_rate']+=1;
                        # his_out_data['freeze_rate']+=_freeze_rate;
                        # his_cdn_data[cdn]['freeze_rate']+=_freeze_rate;
                        # his_cdn_count[cdn]['freeze_rate']+=1;
                        his_count['freeze_rate'] += f_sum
                        his_out_data['freeze_rate'] += f_count
                        his_cdn_data[cdn]['freeze_rate'] += f_count
                        his_cdn_count[cdn]['freeze_rate'] += f_sum
                        # } liupan modify, 2018/5/31
                        # { liupan add, 2018/6/14
                        his_out_data['ps_freeze_rate'] += pf_count
                        his_cdn_data[cdn]['ps_freeze_rate'] += pf_count
                        # } liupan add, 2018/6/14
                        #print("f_sum=%d f_count=%d _freeze_rate=%f"%(f_sum,f_count,_freeze_rate))
                    if b_sum != 0:
                        _bitrate = b_count / b_sum
                        his_count['bitrate'] += b_sum
                        his_out_data['bitrate'] += b_count
                        his_cdn_data[cdn]['bitrate'] += b_count
                        his_cdn_count[cdn]['bitrate'] += b_sum

                    # add by qjk
                    # 增加freeze_avg_iv 与 delayed_avg的统计
                    if f_agv_map['f_avg_n'] != 0:
                        his_count['freeze_avg_n'] += f_agv_map['f_avg_n']
                        his_out_data['freeze_avg_iv'] += f_agv_map['f_avg']
                        his_cdn_count[cdn]['freeze_avg_n'] += f_agv_map['f_avg_n']
                        his_cdn_data[cdn]['freeze_avg_iv'] += f_agv_map['f_avg']
                    if d_agv_map['d_avg_n'] != 0:
                        his_count['delayed_avg_n'] += d_agv_map['d_avg_n']
                        his_out_data['delayed_avg'] += d_agv_map['d_avg']
                        his_cdn_count[cdn]['delayed_avg_n'] += d_agv_map['d_avg_n']
                        his_cdn_data[cdn]['delayed_avg'] += d_agv_map['d_avg']
                    # { liupan modify, 2018/6/1
                    # # { liupan modify, 2018/5/31
                    # # node_detail={"cdn":cdn,"region":region,"operator":operator,"vip":vip_flags,"freeze_rate":_freeze_rate,"success_rate":_success_rate,"bitrate":_bitrate,"band_width":_band_width,"level":_level,"ip":node_ip_detail,'bit_sum':b_count,'bit_time':b_sum};
                    # node_detail={"cdn":cdn,"region":region,"operator":operator,"vip":vip_flags,"freeze_rate":f_count,"freeze_rate_sum":f_sum,"success_rate":_success_rate,"bitrate":_bitrate,"band_width":_band_width,"level":_level,"ip":node_ip_detail,'bit_sum':b_count,'bit_time':b_sum};
                    # # } liupan modify, 2018/5/31

                    # # { liupan modify, 2018/5/31
                    # # # { liupan modify, 2017/8/17
                    # # # history_data_single={"time":start,"region":region,"cdn":cdn,"operator":operator,"freeze_rate":_freeze_rate,"bitrate":_bitrate,"success_rate":_success_rate,"band_width":_band_width,"level":_level,"vip":vip_flags,'bit_sum':b_count,'bit_time':b_sum};
                    # # history_data_single={"time":start,"region":region,"cdn":cdn,"operator":operator,"freeze_rate":_freeze_rate,"bitrate":_bitrate,"success_rate":_success_rate,"band_width":_band_width,"sy_band_width":_sy_band_width,"level":_level,"vip":vip_flags,'bit_sum':b_count,'bit_time':b_sum};
                    # # # } liupan modify, 2017/8/17
                    # history_data_single={"time":start,"region":region,"cdn":cdn,"operator":operator,"freeze_rate":f_count,"freeze_rate_sum":f_sum,"bitrate":_bitrate,"success_rate":_success_rate,"band_width":_band_width,"sy_band_width":_sy_band_width,"level":_level,"vip":vip_flags,'bit_sum':b_count,'bit_time':b_sum};
                    # # } liupan modify, 2018/5/31
                    node_detail = {
                        "cdn": cdn,
                        "region": region,
                        "operator": operator,
                        "vip": vip_flags,
                        "freeze_rate": f_count,
                        "freeze_rate_sum": f_sum,
                        "success_rate": s_count,
                        "success_rate_sum": s_sum,
                        "bitrate": _bitrate,
                        "band_width": _band_width,
                        "level": _level,
                        "ip": node_ip_detail,
                        'bit_sum': b_count,
                        'bit_time': b_sum,
                        'duration': _duration,
                        'jam_all': _jam_all
                    }
                    history_data_single = {
                        "time": start,
                        "region": region,
                        "cdn": cdn,
                        "operator": operator,
                        "freeze_rate": f_count,
                        "freeze_rate_sum": f_sum,
                        "bitrate": _bitrate,
                        "success_rate": s_count,
                        "success_rate_sum": s_sum,
                        "band_width": _band_width,
                        "sy_band_width": _sy_band_width,
                        "level": _level,
                        "vip": vip_flags,
                        'bit_sum': b_count,
                        'bit_time': b_sum,
                        'duration': _duration,
                        'jam_all': _jam_all
                    }
                    # } liupan modify, 2018/6/1
                    # { liupan add, 2018/6/12
                    history_data_single['user_n'] = channel_dic['user_n']
                    history_data_single['req_n'] = channel_dic['req_n']
                    # } liupan add, 2018/6/12
                    # { liupan add, 2018/6/14
                    history_data_single['ps_freeze_rate'] = pf_count
                    # } liupan add, 2018/6/14

                    # add by qjk
                    history_data_single['freeze_avg_iv'] = f_agv_map['f_avg']
                    history_data_single['freeze_avg_n'] = f_agv_map['f_avg_n']
                    history_data_single['delayed_avg'] = d_agv_map['d_avg']
                    history_data_single['delayed_avg_n'] = d_agv_map['d_avg_n']

                    node_detail['freeze_avg_iv'] = f_agv_map['f_avg']
                    node_detail['freeze_avg_n'] = f_agv_map['f_avg_n']
                    node_detail['delayed_avg'] = d_agv_map['d_avg']
                    node_detail['delayed_avg_n'] = d_agv_map['d_avg_n']

                    # { liupan modify, 2017/8/1
                    # history_in.insert_one(history_data_single);
                    queue_history_single.put(
                        history_data_single)  # 某个用户的所有频道数据
                    history_single_count[count_index] += 1
                    # } liupan modify, 2017/8/1
                    write_db["user_statistics"].append(node_detail)

    # { liupan modify, 2017/8/1
    # ret = user_in.insert_one(write_db)
    queue_statistic.put(write_db)
    # } liupan modify, 2017/8/1

 # {
    freeze_rate_out = 0
    success_rate_out = 0
    bitrate_out = 0
    # { liupan delete, 2018/5/31
    # if his_count['freeze_rate']!=0:
    #     freeze_rate_out=his_out_data['freeze_rate']/his_count['freeze_rate'];
    # } liupan delete, 2018/5/31
    if his_count['bitrate'] != 0:
        bitrate_out = his_out_data['bitrate'] / his_count['bitrate']
    # { liupan delete, 2018/6/1
    # if his_count['success_rate']!=0:
    #     success_rate_out=his_out_data['success_rate']/his_count['success_rate'];
    # } liupan delete, 2018/6/1
    # { liupan modify, 2018/6/1
    # # { liupan modify, 2018/5/31
    # # history_data={"time":start,"region":'all',"cdn":{},"operator":'all',"freeze_rate":freeze_rate_out,"bitrate":bitrate_out,"success_rate":success_rate_out,"band_width":his_out_data['band_width'],'bit_sum':his_out_data['bitrate'],'bit_time':his_count['bitrate']};
    # history_data={"time":start,"region":'all',"cdn":{},"operator":'all',"freeze_rate":his_out_data['freeze_rate'],"freeze_rate_sum":his_count['freeze_rate'],"bitrate":bitrate_out,"success_rate":success_rate_out,"band_width":his_out_data['band_width'],'bit_sum':his_out_data['bitrate'],'bit_time':his_count['bitrate']};
    # # } liupan modify, 2018/5/31
    history_data = {
        'time': start,
        'region': 'all',
        'cdn': {},
        'operator': 'all',
        'bitrate': bitrate_out,
        'band_width': his_out_data['band_width'],
        'user_n': his_out_data['user_n'],
        'req_n': his_out_data['req_n'],
        'duration': his_out_data['duration'],
        'jam_all': his_out_data['jam_all'],
        'bit_sum': his_out_data['bitrate'],
        'bit_time': his_count['bitrate'],
        'freeze_rate': his_out_data['freeze_rate'],
        'freeze_rate_sum': his_count['freeze_rate'],
        'success_rate': his_out_data['success_rate'],
        'success_rate_sum': his_count['success_rate'],
        'ps_freeze_rate': his_out_data['ps_freeze_rate'],
        'edge_band_width': edge_data['band_width']
    }

    # { liupan add, 2018/6/14
    # history_data['ps_freeze_rate'] = his_out_data['ps_freeze_rate']
    # } liupan add, 2018/6/14
    # history_data['edge_band_width'] = edge_data['band_width']
    history_data['agent'] = agent_count_dic
    history_data['agent']['all']['bitrate'] = bitrate_out
    history_data['agent']['all']['bit_sum'] = his_out_data['bitrate']
    history_data['agent']['all']['bit_time'] = his_count['bitrate']
    history_data['agent']['all']['band_width'] = his_out_data['band_width']
    history_data['agent']['all']['edge_band_width'] = edge_data['band_width']
    history_data['agent']['all']['user_n'] = his_out_data['user_n']
    history_data['agent']['all']['req_n'] = his_out_data['req_n']
    history_data['agent']['all']['duration'] = his_out_data['duration']
    history_data['agent']['all']['jam_all'] = his_out_data['jam_all']
    history_data['agent']['all']['delayed_avg'] = his_out_data['delayed_avg']
    history_data['agent']['all']['delayed_avg_n'] = his_count['delayed_avg_n']
    history_data['agent']['all']['freeze_avg_iv'] = his_out_data['freeze_avg_iv']
    history_data['agent']['all']['freeze_avg_n'] = his_count['freeze_avg_n']
    # { liupan add, 2017/8/17
    history_data['sy_band_width'] = sy_data['band_width']
    history_data['agent']['all']['sy_band_width'] = sy_data['band_width']
    # } liupan add, 2017/8/17

    # add by qjk
    if his_count['freeze_avg_n'] != 0:
        history_data['freeze_avg_iv'] = his_out_data['freeze_avg_iv']
        history_data['freeze_avg_n'] = his_count['freeze_avg_n']
    else:
        history_data['freeze_avg_iv'] = 0
        history_data['freeze_avg_n'] = 0

    if his_count['delayed_avg_n'] != 0:
        history_data['delayed_avg'] = his_out_data['delayed_avg']
        history_data['delayed_avg_n'] = his_count['delayed_avg_n']
    else:
        history_data['delayed_avg'] = 0
        history_data['delayed_avg_n'] = 0

    for _cdn in his_cdn_data:
        # print(_cdn);
        cdn_bit_sum_temp = 0
        # { liupan delete, 2018/5/31
        # if his_cdn_count[_cdn]['freeze_rate']!=0:
        #         ftemp=his_cdn_data[_cdn]['freeze_rate']
        #         his_cdn_data[_cdn]['freeze_rate']=his_cdn_data[_cdn]['freeze_rate']/his_cdn_count[_cdn]['freeze_rate'];
        #         #print("his_cdn_count:%f %d %f"%(ftemp,his_cdn_count[_cdn]['freeze_rate'],his_cdn_data[_cdn]['freeze_rate']))
        # } liupan delete, 2018/5/31
        # { liupan delete, 2018/6/1
        # if his_cdn_count[_cdn]['success_rate']!=0:
        #         his_cdn_data[_cdn]['success_rate']=his_cdn_data[_cdn]['success_rate']/his_cdn_count[_cdn]['success_rate'];
        # } liupan delete, 2018/6/1
        if his_cdn_count[_cdn]['bitrate'] != 0:
            cdn_bit_sum_temp = his_cdn_data[_cdn]['bitrate']
            his_cdn_data[_cdn]['bitrate'] = his_cdn_data[_cdn]['bitrate'] / \
                his_cdn_count[_cdn]['bitrate']
        # { liupan modify, 2018/6/1
        # # { liupan modify, 2018/5/31
        # # history_data['cdn'][_cdn]={"freeze_rate":his_cdn_data[_cdn]['freeze_rate'],"bitrate":his_cdn_data[_cdn]['bitrate'],"success_rate":his_cdn_data[_cdn]['success_rate'],"band_width":his_cdn_data[_cdn]['band_width'],'bit_sum':cdn_bit_sum_temp,'bit_time':his_cdn_count[_cdn]['bitrate']};
        # history_data['cdn'][_cdn]={"freeze_rate":his_cdn_data[_cdn]['freeze_rate'],"freeze_rate_sum":his_cdn_count[_cdn]['freeze_rate'],"bitrate":his_cdn_data[_cdn]['bitrate'],"success_rate":his_cdn_data[_cdn]['success_rate'],"band_width":his_cdn_data[_cdn]['band_width'],'bit_sum':cdn_bit_sum_temp,'bit_time':his_cdn_count[_cdn]['bitrate']};
        # # } liupan modify, 2018/5/31
        history_data['cdn'][_cdn] = {
            "freeze_rate": his_cdn_data[_cdn]['freeze_rate'],
            "freeze_rate_sum": his_cdn_count[_cdn]['freeze_rate'],
            "bitrate": his_cdn_data[_cdn]['bitrate'],
            "success_rate": his_cdn_data[_cdn]['success_rate'],
            "success_rate_sum": his_cdn_count[_cdn]['success_rate'],
            "band_width": his_cdn_data[_cdn]['band_width'],
            'bit_sum': cdn_bit_sum_temp,
            'bit_time': his_cdn_count[_cdn]['bitrate']}
        # } liupan modify, 2018/6/1
        history_data['agent'][_cdn]["bitrate"] = his_cdn_data[_cdn]['bitrate']
        history_data['agent'][_cdn]['bit_sum'] = cdn_bit_sum_temp
        history_data['agent'][_cdn]['bit_time'] = his_cdn_count[_cdn]['bitrate']
        history_data['agent'][_cdn]["band_width"] = his_cdn_data[_cdn]['band_width']
        # { liupan add, 2018/6/12
        history_data['cdn'][_cdn]['user_n'] = his_cdn_data[_cdn]['user_n']
        history_data['cdn'][_cdn]['req_n'] = his_cdn_data[_cdn]['req_n']
        history_data['agent'][_cdn]['user_n'] = his_cdn_data[_cdn]['user_n']
        history_data['agent'][_cdn]['req_n'] = his_cdn_data[_cdn]['req_n']

        history_data['cdn'][_cdn]['duration'] = his_cdn_data[_cdn]['duration']
        history_data['cdn'][_cdn]['jam_all'] = his_cdn_data[_cdn]['jam_all']
        history_data['agent'][_cdn]['duration'] = his_cdn_data[_cdn]['duration']
        history_data['agent'][_cdn]['jam_all'] = his_cdn_data[_cdn]['jam_all']
        # } liupan add, 2018/6/12
        # { liupan add, 2018/6/14
        history_data['cdn'][_cdn]['ps_freeze_rate'] = his_cdn_data[_cdn]['ps_freeze_rate']
        # } liupan add, 2018/6/14

        # add by qjk
        history_data['cdn'][_cdn]['freeze_avg_iv'] = his_cdn_data[_cdn]['freeze_avg_iv']
        history_data['cdn'][_cdn]['freeze_avg_n'] = his_cdn_count[_cdn]['freeze_avg_n']
        history_data['cdn'][_cdn]['delayed_avg'] = his_cdn_data[_cdn]['delayed_avg']
        history_data['cdn'][_cdn]['delayed_avg_n'] = his_cdn_count[_cdn]['delayed_avg_n']

        history_data['agent'][_cdn]['freeze_avg_iv'] = his_cdn_data[_cdn]['freeze_avg_iv']
        history_data['agent'][_cdn]['freeze_avg_n'] = his_cdn_count[_cdn]['freeze_avg_n']
        history_data['agent'][_cdn]['delayed_avg'] = his_cdn_data[_cdn]['delayed_avg']
        history_data['agent'][_cdn]['delayed_avg_n'] = his_cdn_count[_cdn]['delayed_avg_n']

    for _cdn in edge_data['cdn']:
        history_data['cdn'][_cdn]['edge_band_width'] = edge_data['cdn'][_cdn]['band_width']
        history_data['agent'][_cdn]["edge_band_width"] = edge_data['cdn'][_cdn]['band_width']
    # { liupan add, 2017/8/17
    for _cdn in sy_data['cdn']:
        history_data['cdn'][_cdn]['sy_band_width'] = sy_data['cdn'][_cdn]['band_width']
        history_data['agent'][_cdn]["sy_band_width"] = sy_data['cdn'][_cdn]['band_width']
    # } liupan add, 2017/8/17

    # print(edge_data);
    # print(history_data);
    # print(history_data)
    # { liupan modify, 2017/8/1
    # history_sum_in.insert_one(history_data);
    queue_history_sum.put(history_data)
    # } liupan modify, 2017/8/1
# }

    for _cdn, channel_dic in his_channel_data.items():
        for _channel in channel_dic:
            # { liupan modify, 2017/8/17
            # channel_out={'cdn':_cdn,'channel':_channel,"time":start,'freeze_rate':0,'bitrate':0,'success_rate':0,'band_width':0,'bit_sum':his_channel_data[_cdn][_channel]['bitrate'],'bit_time':his_channel_count[_cdn][_channel]['bitrate']};
            channel_out = {
                'cdn': _cdn,
                'channel': _channel,
                "time": start,
                'bitrate': 0,
                'sy_band_width': his_channel_data[_cdn][_channel]['sy_band_width'],
                'bit_sum': his_channel_data[_cdn][_channel]['bitrate'],
                'bit_time': his_channel_count[_cdn][_channel]['bitrate'],
                'duration': his_channel_data[_cdn][_channel]['duration'],
                'jam_all': his_channel_data[_cdn][_channel]['jam_all'],
                'success_rate': his_channel_data[_cdn][_channel]['success_rate'],     # liupan modify, 2018/6/1
                'success_rate_sum': his_channel_count[_cdn][_channel]['success_rate'],
                'freeze_rate': his_channel_data[_cdn][_channel]['freeze_rate'],
                'freeze_rate_sum': his_channel_count[_cdn][_channel]['freeze_rate'],  # liupan modify, 2018/5/31
                'band_width': his_channel_data[_cdn][_channel]['band_width'],         # liupan add, 2018/1/18
                'ps_freeze_rate': his_channel_data[_cdn][_channel]['ps_freeze_rate']  # liupan add, 2018/6/14
            }
            # if his_channel_count[_cdn][_channel]['success_rate']!=0:
            #     channel_out['success_rate']=his_channel_data[_cdn][_channel]['success_rate']/his_channel_count[_cdn][_channel]['success_rate'];
            # if his_channel_count[_cdn][_channel]['freeze_rate']!=0:
            #     channel_out['freeze_rate']=his_channel_data[_cdn][_channel]['freeze_rate']/his_channel_count[_cdn][_channel]['freeze_rate'];
            channel_out['freeze_rate'] = his_channel_data[_cdn][_channel]['freeze_rate']
            channel_out['freeze_rate_sum'] = his_channel_count[_cdn][_channel]['freeze_rate']
            # } liupan modify, 2018/5/31
            if his_channel_count[_cdn][_channel]['bitrate'] != 0:
                channel_out['bitrate'] = his_channel_data[_cdn][_channel]['bitrate'] / \
                    his_channel_count[_cdn][_channel]['bitrate']
            channel_out['band_width'] = his_channel_data[_cdn][_channel]['band_width']
            # { liupan add, 2018/1/18
            if _cdn in agent_channel_dic and _channel in agent_channel_dic[_cdn]:
                channel_out['agent'] = agent_channel_dic[_cdn][_channel]
            else:
                channel_out['agent'] = {}

            # channel_sum_in.insert_one(channel_out);

            # add by qjk
            if 'freeze_avg_iv' in his_channel_data[_cdn][_channel]:
                channel_out['freeze_avg_iv'] = his_channel_data[_cdn][_channel]['freeze_avg_iv']
            else:
                channel_out['freeze_avg_iv'] = 0
            if 'freeze_avg_n' in his_channel_data[_cdn][_channel]:
                channel_out['freeze_avg_n'] = his_channel_data[_cdn][_channel]['freeze_avg_n']
            else:
                channel_out['freeze_avg_n'] = 0

            if 'delayed_avg' in his_channel_data[_cdn][_channel]:
                channel_out['delayed_avg'] = his_channel_data[_cdn][_channel]['delayed_avg']
            else:
                channel_out['delayed_avg'] = 0

            if 'delayed_avg_n' in his_channel_data[_cdn][_channel]:
                channel_out['delayed_avg_n'] = his_channel_data[_cdn][_channel]['delayed_avg_n']
            else:
                channel_out['delayed_avg_n'] = 0

            queue_channel_sum.put(channel_out)
            channel_sum_count[count_index] += 1
            # } liupan modify, 2017/8/1

    #logger.info("pid=%d Insert End %d"%(os.getpid(),time_temp));
    # { liupan delete, 2017/8/1
    # main_client.close();
    # } liupan delete, 2017/8/1

    # { liupan add, 2017/8/1
    p_queue.put(channel_bitrate_dic)
    # } liupan add, 2017/8/1

    time_temp = int(time.time()) - time_now
    #logger.info("pid=%d append time=%d"%(os.getpid(),time_temp));
    logger.info('pid=%d process End.' % os.getpid())


def push_into_mongo(p_queue, p_count, start_time, process_list):
    channel_push_dic = {}
    get_count = 0
    write_flag = 1
    try:
        while get_count < p_count:
            dic_temp = p_queue.get(True, 30)
            get_count += 1
            # print(dic);
            for cdn_temp, channel_dic in dic_temp.items():
                if not(cdn_temp in channel_push_dic):
                    channel_push_dic[cdn_temp] = {}
                for channel_name, bit_dic in channel_dic.items():
                    if not (channel_name in channel_push_dic[cdn_temp]):
                        # { liupan modify, 2017/8/1
                        # if statis_version=='CNTV':
                        #     channel_push_dic[cdn_temp][channel_name]={'0':0,'1':0,'2':0,'3':0,'4':0};
                        # elif statis_version=='LD_KW':
                        #     channel_push_dic[cdn_temp][channel_name]={};
                        # # { liupan add, 2017/7/11
                        # elif statis_version == 'JAPAN':
                        #     channel_push_dic[cdn_temp][channel_name] = {};
                        # # } liupan add, 2017/7/11
                        channel_push_dic[cdn_temp][channel_name] = {}
                        # } liupan modify, 2017/8/1
                    for index_bit, n_bit in bit_dic.items():
                        # { liupan modify, 2017/8/1
                        # if statis_version=='LD_KW':
                        #     if not( index_bit in channel_push_dic[cdn_temp][channel_name]):
                        #         channel_push_dic[cdn_temp][channel_name][index_bit]=0;
                        # # { liupan add, 2017/7/11
                        # elif statis_version == 'JAPAN':
                        #     if not (index_bit in channel_push_dic[cdn_temp][channel_name]):
                        #         channel_push_dic[cdn_temp][channel_name][index_bit] = 0;
                        # # } liupan add, 2017/7/11
                        if not(
                                index_bit in channel_push_dic[cdn_temp][channel_name]):
                            channel_push_dic[cdn_temp][channel_name][index_bit] = 0
                        # } liupan modify, 2017/8/1
                        channel_push_dic[cdn_temp][channel_name][index_bit] += n_bit
            #print("queue empty situation:",p_queue.empty());
            logger.info("queue get %d success,all:%d." % (get_count, p_count))
            # time.sleep(0.1);
        else:
            logger.info("get queue Ok!")
    except queue.Empty:
        write_flag = 0
        logger.info("p_queue empty.")
    else:
        pass
    # print(channel_push_dic);
    # { liupan delete, 2017/8/21
    # i_test=0;
    # for p in process_list:
    #     while p.is_alive():
    #         time.sleep(1);
    #         logger.info("join %d sleep."%i_test);
    #     else:
    #         logger.info("join %d success."%i_test);
    #         i_test+=1
    # } liupan delete, 2017/8/21

    # { liupan modify, 2017/7/13
    # if statis_version=='CNTV':
    #    main_client=MongoClient("192.168.2.30", 27017)
    #    main_db=main_client.cntv_log_db
    # elif statis_version=='LD_KW':
    #    main_client=MongoClient("10.27.219.220", 27017)
    #    main_db=main_client.ld_log_db
    # { liupan add, 2017/7/11
    # elif statis_version == 'JAPAN':
    #    main_client = MongoClient("10.27.219.220", 27017);      # ???
    #    main_db = main_client.jp_log_db;
    # } liupan add, 2017/7/11
    # { liupan modify, 2018/4/24
    # main_client = MongoClient(mongodb_main_ip, mongodb_main_port);
    if mongodb_user == '':
        main_client = MongoClient(mongodb_main_ip, mongodb_main_port)
    else:
        main_client = MongoClient(
            'mongodb://%s:%s@%s:%s/default_db?authSource=admin' %
            (mongodb_user, mongodb_pwd, mongodb_main_ip, mongodb_main_port))
    # } liupan modify, 2018/4/24
    main_db = main_client.get_database(mongodb_db)
    # } liupan modify, 2017/7/13

    user_in = main_db.user_channel_table
    if write_flag == 1:
        for _cdn, channel_dic in channel_push_dic.items():
            cdn_channel_struct = {
                'cdn': _cdn,
                'channel': channel_dic,
                'time': start_time}
            # print(cdn_channel_struct);
            user_in.insert_one(cdn_channel_struct)
    logger.info("push_into_mongo End!")
    logger.info('get count =%d p_count=%d' % (get_count, p_count))
    main_client.close()


def sum_data(str_key, str_key_n, src_data, det_data):
    if str_key in src_data:
        det_data[str_key] += src_data[str_key]
    if str_key_n in src_data:
        det_data[str_key_n] += src_data[str_key_n]


def cal_avg(str_div, str_divd, src_data, is_del_div):
    if str_div in src_data:
        if src_data[str_div] != 0:
            src_data[str_divd] = src_data[str_divd] / src_data[str_div]
        else:
            src_data[str_divd] = 0

        if is_del_div:
            del src_data[str_div]

# { liupan add, 2017/8/1


def push_into_mongo2(
        queue_statistic,
        queue_history_single,
        queue_history_sum,
        queue_channel_sum,
        queue_history_user_by_node,
        process_count,
        history_single_count_all,
        channel_sum_count_all,
        history_user_by_node_count_all):
    #############################################
    ## 计算history single和channel sum的总条数 ##
    #############################################

    history_single_count = 0
    channel_sum_count = 0
    history_user_by_node_count = 0
    for x in history_single_count_all:
        # v = history_single_count_all.get(x);
        # history_single_count += v;
        history_single_count += x
    for x in channel_sum_count_all:
        # v = channel_sum_count_all.get(x);
        # channel_sum_count += v;
        channel_sum_count += x
    for x in history_user_by_node_count_all:
        history_user_by_node_count += x

    ################
    ## 连接数据库 ##
    ################
    # { liupan modify, 2018/4/24
    # main_client = MongoClient(mongodb_main_ip, mongodb_main_port);
    if mongodb_user == '':
        main_client = MongoClient(mongodb_main_ip, mongodb_main_port)
    else:
        main_client = MongoClient(
            'mongodb://%s:%s@%s:%s/default_db?authSource=admin' %
            (mongodb_user, mongodb_pwd, mongodb_main_ip, mongodb_main_port))
    # } liupan modify, 2018/4/24
    main_db = main_client.get_database(mongodb_db)
    user_in = main_db.statistic_user
    history_in = main_db.history_user_single
    history_sum_in = main_db.history_user_sum
    channel_sum_in = main_db.user_channel_cdn_single
    history_user_by_node = main_db.history_user_by_node

    #######################
    ## 合并statistic数据 ##
    #######################
    get_count = 0
    # 合并之后的数据
    statistic_db = {"user_statistics": [], "time": start}
    # 记录每种类型的合并条数，用于求平均值
    all_count_s = []
    try:
        while get_count < process_count:
            if get_count == 0:
                # 从队列中取数据
                statistic_db = queue_statistic.get(True, 30)
                for statistic_db_x in statistic_db["user_statistics"]:
                    all_count_s.append(1)
                get_count += 1
            else:
                # 从队列中取数据
                temp_db = queue_statistic.get(True, 30)
                get_count += 1
                for temp_db_x in temp_db["user_statistics"]:
                    index = -1
                    count = 0
                    # 查找当前是否有该类型的数据
                    for statistic_db_x in statistic_db["user_statistics"]:
                        if statistic_db_x["cdn"] == temp_db_x["cdn"] and statistic_db_x["region"] == temp_db_x[
                                "region"] and statistic_db_x["operator"] == temp_db_x["operator"] and statistic_db_x["vip"] == temp_db_x["vip"]:
                            index = count
                            break
                        count += 1
                    # 没有该类型的数据，直接加入
                    if index == -1:
                        statistic_db["user_statistics"].append(temp_db_x)
                        all_count_s.append(1)
                    # 有该类型的数据，进行合并
                    else:
                        all_count_s[index] += 1
                        # 由于统计时的代码已经写死了这些key名，所以使用前不用判断key是否存在。
                        statistic_db["user_statistics"][index]["freeze_rate"] += temp_db_x["freeze_rate"]
                        # { liupan add, 2018/5/31
                        statistic_db["user_statistics"][index]["freeze_rate_sum"] += temp_db_x["freeze_rate_sum"]
                        # } liupan add, 2018/5/31
                        statistic_db["user_statistics"][index]["success_rate"] += temp_db_x["success_rate"]
                        # { liupan add, 2018/6/1
                        statistic_db["user_statistics"][index]["success_rate_sum"] += temp_db_x["success_rate_sum"]
                        # } liupan add, 2018/6/1
                        statistic_db["user_statistics"][index]["bitrate"] += temp_db_x["bitrate"]
                        statistic_db["user_statistics"][index]["band_width"] += temp_db_x["band_width"]
                        statistic_db["user_statistics"][index]["bit_sum"] += temp_db_x["bit_sum"]
                        statistic_db["user_statistics"][index]["bit_time"] += temp_db_x["bit_time"]

                        # add by qjk
                        sum_data(
                            'freeze_avg_iv',
                            'freeze_avg_n',
                            temp_db_x,
                            statistic_db["user_statistics"][index])
                        sum_data(
                            'delayed_avg',
                            'delayed_avg_n',
                            temp_db_x,
                            statistic_db["user_statistics"][index])

                        for ip1 in temp_db_x["ip"]:
                            statistic_db["user_statistics"][index]["ip"].append(
                                ip1)
        else:
            logger.info("get queue_statistic Ok!")
    except queue.Empty:
        logger.info("queue_statistic empty.")
    else:
        pass
    # 求平均
    for x in range(len(statistic_db["user_statistics"])):
        # { liupan modify, 2018/5/31
        # if all_count_s[x] == 0:
        #     statistic_db["user_statistics"][x]["freeze_rate"] = 0;
        #     statistic_db["user_statistics"][x]["success_rate"] = 0;
        # else:
        #     statistic_db["user_statistics"][x]["freeze_rate"] /= all_count_s[x];
        #     statistic_db["user_statistics"][x]["success_rate"] /= all_count_s[x];
        if statistic_db["user_statistics"][x]["freeze_rate_sum"] == 0:
            statistic_db["user_statistics"][x]["freeze_rate"] = 0
        else:
            statistic_db["user_statistics"][x]["freeze_rate"] *= 100
            statistic_db["user_statistics"][x]["freeze_rate"] /= statistic_db["user_statistics"][x]["freeze_rate_sum"]
        # { liupan modify, 2018/6/1
        # statistic_db["user_statistics"][x]["success_rate"] /= all_count_s[x];
        if statistic_db["user_statistics"][x]["success_rate_sum"] == 0:
            statistic_db["user_statistics"][x]["success_rate"] = 0
        else:
            statistic_db["user_statistics"][x]["success_rate"] *= 100
            statistic_db["user_statistics"][x]["success_rate"] /= statistic_db["user_statistics"][x]["success_rate_sum"]
        del statistic_db["user_statistics"][x]["success_rate_sum"]
        # } liupan modify, 2018/6/1
        del statistic_db["user_statistics"][x]["freeze_rate_sum"]
        # } liupan modify, 2018/5/31
        if statistic_db["user_statistics"][x]["bit_time"] == 0:
            statistic_db["user_statistics"][x]["bitrate"] = 0
        else:
            statistic_db["user_statistics"][x]["bitrate"] = statistic_db["user_statistics"][x]["bit_sum"] / \
                statistic_db["user_statistics"][x]["bit_time"]

        # add by qjk
        # 增加对平均卡顿间隔的统计
        cal_avg(
            'freeze_avg_n',
            'freeze_avg_iv',
            statistic_db["user_statistics"][x],
            True)
        # 增加对平均延时的统计
        cal_avg(
            'delayed_avg_n',
            'delayed_avg',
            statistic_db["user_statistics"][x],
            True)

    # 插入数据库
    user_in.insert_one(statistic_db)
    logger.info(
        'statistic_db get count =%d p_count=%d' %
        (get_count, process_count))

    ############################
    ## 合并history single数据 ##
    ############################
    get_count = 0
    # 记录合并之后的数据
    history_single_db = []
    # 记录每种类型的合并条数，用于求平均值
    all_count_hs = []
    try:
        while get_count < history_single_count:
            if get_count == 0:
                # 从队列中取数据
                history_single_db.append(queue_history_single.get(True, 30))
                all_count_hs.append(1)
                get_count += 1
            else:
                # 从队列中取数据
                temp_db = queue_history_single.get(True, 30)
                get_count += 1
                index = -1
                count = 0
                # 查找当前是否有该类型的数据
                for history_single_db_x in history_single_db:
                    if history_single_db_x["cdn"] == temp_db["cdn"] and history_single_db_x["region"] == temp_db[
                            "region"] and history_single_db_x["operator"] == temp_db["operator"] and history_single_db_x["vip"] == temp_db["vip"]:
                        index = count
                        break
                    count += 1
                # 没有该类型的数据，直接加入
                if index == -1:
                    history_single_db.append(temp_db)
                    all_count_hs.append(1)
                # 有该类型的数据，进行合并
                else:
                    all_count_hs[index] += 1
                    # 由于统计时的代码已经写死了这些key名，所以使用前不用判断key是否存在。
                    history_single_db[index]["freeze_rate"] += temp_db["freeze_rate"]
                    # { liupan add, 2018/5/31
                    history_single_db[index]["freeze_rate_sum"] += temp_db["freeze_rate_sum"]
                    # } liupan add, 2018/5/31
                    history_single_db[index]["bitrate"] += temp_db["bitrate"]
                    history_single_db[index]["success_rate"] += temp_db["success_rate"]
                    # { liupan add, 2018/6/1
                    history_single_db[index]["success_rate_sum"] += temp_db["success_rate_sum"]
                    # } liupan add, 2018/6/1
                    history_single_db[index]["band_width"] += temp_db["band_width"]
                    history_single_db[index]["bit_sum"] += temp_db["bit_sum"]
                    history_single_db[index]["bit_time"] += temp_db["bit_time"]
                    # { liupan add, 2017/8/17
                    history_single_db[index]["sy_band_width"] += temp_db["sy_band_width"]
                    # } liupan add, 2017/8/17
                    # { liupan add, 2018/6/12
                    history_single_db[index]["user_n"] += temp_db["user_n"]
                    history_single_db[index]["req_n"] += temp_db["req_n"]
                    # } liupan add, 2018/6/12
                    # { liupan add, 2018/6/14
                    history_single_db[index]["ps_freeze_rate"] += temp_db["ps_freeze_rate"]
                    # } liupan add, 2018/6/14

                    # add by qjk
                    sum_data(
                        'freeze_avg_iv',
                        'freeze_avg_n',
                        temp_db,
                        history_single_db[index])
                    sum_data(
                        'delayed_avg',
                        'delayed_avg_n',
                        temp_db,
                        history_single_db[index])

        else:
            logger.info("get queue_history_single Ok!")
    except queue.Empty:
        logger.info("queue_history_single empty.")
    else:
        pass
    # 求平均，并插入数据库
    for index in range(len(history_single_db)):
        # { liupan modify, 2018/5/31
        # if all_count_hs[index] == 0:
        #     history_single_db[index]["freeze_rate"] = 0;
        #     history_single_db[index]["success_rate"] = 0;
        # else:
        #     history_single_db[index]["freeze_rate"] /= all_count_hs[index];
        #     history_single_db[index]["success_rate"] /= all_count_hs[index];
        if history_single_db[index]["freeze_rate_sum"] == 0:
            history_single_db[index]["freeze_rate"] = 0
            # { liupan add, 2018/6/14
            history_single_db[index]["ps_freeze_rate"] = 0
            # } liupan add, 2018/6/14
        else:
            history_single_db[index]["freeze_rate"] *= 100
            history_single_db[index]["freeze_rate"] /= history_single_db[index]["freeze_rate_sum"]
            # { liupan add, 2018/6/14
            history_single_db[index]["ps_freeze_rate"] *= 100
            history_single_db[index]["ps_freeze_rate"] /= history_single_db[index]["freeze_rate_sum"]
            # } liupan add, 2018/6/14
        # { liupan modify, 2018/6/1
        # history_single_db[index]["success_rate"] /= all_count_hs[index];
        if history_single_db[index]["success_rate_sum"] == 0:
            history_single_db[index]["success_rate"] = 0
        else:
            history_single_db[index]["success_rate"] *= 100
            history_single_db[index]["success_rate"] /= history_single_db[index]["success_rate_sum"]
        del history_single_db[index]["success_rate_sum"]
        # } liupan modify, 2018/6/1
        del history_single_db[index]["freeze_rate_sum"]
        # } liupan modify, 2018/5/31
        if history_single_db[index]["bit_time"] == 0:
            history_single_db[index]["bitrate"] = 0
        else:
            history_single_db[index]["bitrate"] = history_single_db[index]["bit_sum"] / \
                history_single_db[index]["bit_time"]

        # add by qjk
        # 增加对平均卡顿间隔的统计
        cal_avg(
            'freeze_avg_n',
            'freeze_avg_iv',
            history_single_db[index],
            True)
        # 增加对平均延时的统计
        cal_avg('delayed_avg_n', 'delayed_avg', history_single_db[index], True)

        # 插入数据库
        history_in.insert_one(history_single_db[index])
    logger.info(
        'history_single_db get count =%d p_count=%d' %
        (get_count, history_single_count))

    #########################
    ## 合并history sum数据 ##
    #########################
    get_count = 0
    # 记录合并之后的数据
    history_sum_db = {}
    # 记录每个cdn的合并条数，用于求平均值
    all_count_hsu = {}
    try:
        while get_count < process_count:
            if get_count == 0:
                # 从队列中取数据
                history_sum_db = queue_history_sum.get(True, 30)
                # 初始化每个cdn的条数为1
                for cdn in history_sum_db["agent"]:
                    all_count_hsu[cdn] = 1
                get_count += 1
            else:
                # 从队列中取数据
                temp_db = queue_history_sum.get(True, 30)
                get_count += 1
                # 由于统计时的代码已经写死了这些key名，所以使用前不用判断key是否存在。
                history_sum_db["freeze_rate"] += temp_db["freeze_rate"]
                # { liupan add, 2018/5/31
                history_sum_db["freeze_rate_sum"] += temp_db["freeze_rate_sum"]
                # } liupan add, 2018/5/31
                history_sum_db["bitrate"] += temp_db["bitrate"]
                history_sum_db["success_rate"] += temp_db["success_rate"]
                # { liupan add, 2018/6/1
                history_sum_db["success_rate_sum"] += temp_db["success_rate_sum"]
                # } liupan add, 2018/6/1
                history_sum_db["band_width"] += temp_db["band_width"]
                history_sum_db["bit_sum"] += temp_db["bit_sum"]
                history_sum_db["bit_time"] += temp_db["bit_time"]
                history_sum_db["edge_band_width"] += temp_db["edge_band_width"]
                # { liupan add, 2017/8/17
                history_sum_db["sy_band_width"] += temp_db["sy_band_width"]
                # } liupan add, 2017/8/17
                # { liupan add, 2018/6/12
                history_sum_db["user_n"] += temp_db["user_n"]
                history_sum_db["req_n"] += temp_db["req_n"]
                # } liupan add, 2018/6/12
                # { liupan add, 2018/6/14
                history_sum_db["ps_freeze_rate"] += temp_db["ps_freeze_rate"]
                # } liupan add, 2018/6/14

               # add by qjk
                sum_data(
                    'freeze_avg_iv',
                    'freeze_avg_n',
                    temp_db,
                    history_sum_db)
                sum_data(
                    'delayed_avg',
                    'delayed_avg_n',
                    temp_db,
                    history_sum_db)

                for cdn1 in temp_db["agent"]:
                    flag = 0
                    # 查找是否已经有该cdn类型，有则进行合并
                    for cdn2 in history_sum_db["agent"]:
                        if cdn1 == cdn2:
                            for type1 in temp_db["agent"][cdn1]:
                                if type1 in history_sum_db["agent"][cdn2]:
                                    history_sum_db["agent"][cdn2][type1] += temp_db["agent"][cdn1][type1]
                                else:
                                    history_sum_db["agent"][cdn2][type1] = temp_db["agent"][cdn1][type1]
                            all_count_hsu[cdn1] += 1
                            flag = 1
                            break
                    # 没有找到该cdn类型，则直接加入
                    if flag == 0:
                        history_sum_db["agent"][cdn1] = temp_db["agent"][cdn1]
                        all_count_hsu[cdn1] = 1

                for cdn1 in temp_db["cdn"]:
                    flag = 0
                    # 查找是否已经有该cdn类型，有则进行合并
                    for cdn2 in history_sum_db["cdn"]:
                        if cdn1 == cdn2:
                            for type1 in temp_db["cdn"][cdn1]:
                                if type1 in history_sum_db["cdn"][cdn2]:
                                    history_sum_db["cdn"][cdn2][type1] += temp_db["cdn"][cdn1][type1]
                                else:
                                    history_sum_db["cdn"][cdn2][type1] = temp_db["cdn"][cdn1][type1]
                            flag = 1
                            break
                    # 没有找到该cdn类型，则直接加入
                    if flag == 0:
                        history_sum_db["cdn"][cdn1] = temp_db["cdn"][cdn1]

        else:
            logger.info("get queue_history_sum Ok!")
    except queue.Empty:
        logger.info("queue_history_sum empty.")
    else:
        pass
    # 求平均
    # { liupan modify, 2018/5/31
    # if get_count == 0:
    #     history_sum_db["freeze_rate"] = 0;
    #     history_sum_db["success_rate"] = 0;
    # else:
    #     history_sum_db["freeze_rate"] /= process_count;
    #     history_sum_db["success_rate"] /= process_count;
    if history_sum_db["freeze_rate_sum"] == 0:
        history_sum_db["freeze_rate"] = 0
        # { liupan add, 2018/6/14
        history_sum_db["ps_freeze_rate"] = 0
        # } liupan add, 2018/6/14
    else:
        history_sum_db["freeze_rate"] *= 100
        history_sum_db["freeze_rate"] /= history_sum_db["freeze_rate_sum"]
        # { liupan add, 2018/6/14
        history_sum_db["ps_freeze_rate"] *= 100
        history_sum_db["ps_freeze_rate"] /= history_sum_db["freeze_rate_sum"]
        # } liupan add, 2018/6/14
    # { liupan modify, 2018/6/1
    # history_sum_db["success_rate"] /= process_count;
    if history_sum_db["success_rate_sum"] == 0:
        history_sum_db["success_rate"] = 0
    else:
        history_sum_db["success_rate"] *= 100
        history_sum_db["success_rate"] /= history_sum_db["success_rate_sum"]
    del history_sum_db["success_rate_sum"]
    # } liupan modify, 2018/6/1
    del history_sum_db["freeze_rate_sum"]
    # } liupan modify, 2018/5/31
    if get_count == 0 or history_sum_db["bit_time"] == 0:
        history_sum_db["bitrate"] = 0
    else:
        history_sum_db["bitrate"] = history_sum_db["bit_sum"] / \
            history_sum_db["bit_time"]

    if get_count > 0:
        for cdn in history_sum_db["cdn"]:
            # { liupan modify, 2018/5/31
            # if all_count_hsu[cdn] == 0:
            #     history_sum_db["cdn"][cdn]["freeze_rate"] = 0;
            #     history_sum_db["cdn"][cdn]["success_rate"] = 0;
            # else:
            #     history_sum_db["cdn"][cdn]["freeze_rate"] /= all_count_hsu[cdn];
            #     history_sum_db["cdn"][cdn]["success_rate"] /= all_count_hsu[cdn];
            if history_sum_db["cdn"][cdn]["freeze_rate_sum"] == 0:
                history_sum_db["cdn"][cdn]["freeze_rate"] = 0
                # { liupan add, 2018/6/14
                history_sum_db["cdn"][cdn]["ps_freeze_rate"] = 0
                # } liupan add, 2018/6/14
            else:
                history_sum_db["cdn"][cdn]["freeze_rate"] *= 100
                history_sum_db["cdn"][cdn]["freeze_rate"] /= history_sum_db["cdn"][cdn]["freeze_rate_sum"]
                # { liupan add, 2018/6/14
                history_sum_db["cdn"][cdn]["ps_freeze_rate"] *= 100
                history_sum_db["cdn"][cdn]["ps_freeze_rate"] /= history_sum_db["cdn"][cdn]["freeze_rate_sum"]
                # } liupan add, 2018/6/14
            # { liupan modify, 2018/6/1
            # history_sum_db["cdn"][cdn]["success_rate"] /= all_count_hsu[cdn];
            if history_sum_db["cdn"][cdn]["success_rate_sum"] == 0:
                history_sum_db["cdn"][cdn]["success_rate"] = 0
            else:
                history_sum_db["cdn"][cdn]["success_rate"] *= 100
                history_sum_db["cdn"][cdn]["success_rate"] /= history_sum_db["cdn"][cdn]["success_rate_sum"]

            if history_sum_db["cdn"][cdn]["freeze_avg_n"] == 0:
                history_sum_db["cdn"][cdn]["freeze_avg_iv"] = 0
            else:
                history_sum_db["cdn"][cdn]["freeze_avg_iv"] /= history_sum_db["cdn"][cdn]["freeze_avg_n"]

            if history_sum_db["cdn"][cdn]["delayed_avg_n"] == 0:
                history_sum_db["cdn"][cdn]["delayed_avg"] = 0
            else:
                history_sum_db["cdn"][cdn]["delayed_avg"] /= history_sum_db["cdn"][cdn]["delayed_avg_n"]


            del history_sum_db["cdn"][cdn]["success_rate_sum"]
            # } liupan modify, 2018/6/1
            del history_sum_db["cdn"][cdn]["freeze_rate_sum"]
            # } liupan modify, 2018/5/31
            if history_sum_db["cdn"][cdn]["bit_time"] == 0:
                history_sum_db["cdn"][cdn]["bitrate"] = 0
            else:
                history_sum_db["cdn"][cdn]["bitrate"] = history_sum_db["cdn"][cdn]["bit_sum"] / \
                    history_sum_db["cdn"][cdn]["bit_time"]
        for cdn in history_sum_db["agent"]:
            if history_sum_db["agent"][cdn]["bit_time"] == 0:
                history_sum_db["agent"][cdn]["bitrate"] = 0
            else:
                history_sum_db["agent"][cdn]["bitrate"] = history_sum_db["agent"][cdn]["bit_sum"] / \
                    history_sum_db["agent"][cdn]["bit_time"]

            if history_sum_db["agent"][cdn]["freeze_avg_n"] == 0:
                history_sum_db["agent"][cdn]["freeze_avg_iv"] = 0
            else:
                history_sum_db["agent"][cdn]["freeze_avg_iv"] /= history_sum_db["agent"][cdn]["freeze_avg_n"]

            if history_sum_db["agent"][cdn]["delayed_avg_n"] == 0:
                history_sum_db["agent"][cdn]["delayed_avg"] = 0
            else:
                history_sum_db["agent"][cdn]["delayed_avg"] /= history_sum_db["agent"][cdn]["delayed_avg_n"]
        # logger.info("history_sum_db: %s", history_sum_db)

    # add by qjk
    # 增加对平均卡顿间隔的统计
    cal_avg('freeze_avg_n', 'freeze_avg_iv', history_sum_db, True)
    # 增加对平均延时的统计
    cal_avg('delayed_avg_n', 'delayed_avg', history_sum_db, True)

    # 插入数据库
    history_sum_in.insert_one(history_sum_db)
    logger.info('history_sum_db get count =%d p_count=%d', get_count, process_count)

    #########################
    ## 合并channel sum数据 ##
    #########################
    get_count = 0
    # 记录合并之后的数据
    channel_sum_db = []
    # 记录每种类型的合并条数，用于求平均值
    all_count_cs = []
    try:
        while get_count < channel_sum_count:
            if get_count == 0:
                # 从队列中取数据
                channel_sum_db.append(queue_channel_sum.get(True, 30))
                all_count_cs.append(1)
                get_count += 1
            else:
                # 从队列中取数据
                temp_db = queue_channel_sum.get(True, 30)
                get_count += 1
                index = -1
                count = 0
                # 查找是否已有该类型的数据
                for channel_sum_db_x in channel_sum_db:
                    if channel_sum_db_x["cdn"] == temp_db["cdn"] and channel_sum_db_x["channel"] == temp_db["channel"]:
                        index = count
                        break
                    count += 1
                # 未找到，直接加入
                if index == -1:
                    channel_sum_db.append(temp_db)
                    all_count_cs.append(1)
                # 找到，进行合并
                else:
                    # 由于统计时的代码已经写死了这些key名，所以使用前不用判断key是否存在。
                    channel_sum_db[index]["freeze_rate"] += temp_db["freeze_rate"]
                    # { liupan add, 2018/5/31
                    channel_sum_db[index]["freeze_rate_sum"] += temp_db["freeze_rate_sum"]
                    # } liupan add, 2018/5/31
                    channel_sum_db[index]["success_rate"] += temp_db["success_rate"]
                    # { liupan add, 2018/6/1
                    channel_sum_db[index]["success_rate_sum"] += temp_db["success_rate_sum"]
                    # } liupan add, 2018/6/1
                    channel_sum_db[index]["bitrate"] += temp_db["bitrate"]
                    channel_sum_db[index]["band_width"] += temp_db["band_width"]
                    # xuzj add 2018-08-09
                    channel_sum_db[index]["duration"] += temp_db["duration"]
                    channel_sum_db[index]["jam_all"] += temp_db["jam_all"]
                    channel_sum_db[index]["bit_sum"] += temp_db["bit_sum"]
                    channel_sum_db[index]["bit_time"] += temp_db["bit_time"]
                    # { liupan add, 2017/8/17
                    channel_sum_db[index]["sy_band_width"] += temp_db["sy_band_width"]
                    # } liupan add, 2017/8/17
                    # { liupan add, 2018/6/14
                    channel_sum_db[index]["ps_freeze_rate"] += temp_db["ps_freeze_rate"]
                    # } liupan add, 2018/6/14

                    # add by qjk
                    sum_data(
                        'freeze_avg_iv',
                        'freeze_avg_n',
                        temp_db,
                        channel_sum_db[index])
                    sum_data(
                        'delayed_avg',
                        'delayed_avg_n',
                        temp_db,
                        channel_sum_db[index])

                    # { liupan add, 2018/1/18
                    for agent_data in temp_db['agent']:
                        if agent_data in channel_sum_db[index]['agent']:
                            channel_sum_db[index]['agent'][agent_data] += temp_db['agent'][agent_data]
                        else:
                            channel_sum_db[index]['agent'][agent_data] = temp_db['agent'][agent_data]
                    # } liupan add, 2018/1/18

                    all_count_cs[index] += 1
        else:
            logger.info("get queue_channel_sum Ok!")
    except queue.Empty:
        logger.info("queue_channel_sum empty.")
    else:
        pass
    # 求平均，并插入数据库
    for index in range(len(channel_sum_db)):
        # { liupan modify, 2018/5/31
        # if all_count_cs[index] == 0:
        #     channel_sum_db[index]["freeze_rate"] = 0;
        #     channel_sum_db[index]["success_rate"] = 0;
        # else:
        #     channel_sum_db[index]["freeze_rate"] /= all_count_cs[index];
        #     channel_sum_db[index]["success_rate"] /= all_count_cs[index];
        if channel_sum_db[index]["freeze_rate_sum"] == 0:
            channel_sum_db[index]["freeze_rate"] = 0
            # { liupan add, 2018/6/14
            channel_sum_db[index]["ps_freeze_rate"] = 0
            # } liupan add, 2018/6/14
        else:
            channel_sum_db[index]["freeze_rate"] *= 100
            channel_sum_db[index]["freeze_rate"] /= channel_sum_db[index]["freeze_rate_sum"]
            # { liupan add, 2018/6/14
            channel_sum_db[index]["ps_freeze_rate"] *= 100
            channel_sum_db[index]["ps_freeze_rate"] /= channel_sum_db[index]["freeze_rate_sum"]
            # } liupan add, 2018/6/14
        # { liupan modify, 2018/6/1
        # channel_sum_db[index]["success_rate"] /= all_count_cs[index];
        if channel_sum_db[index]["success_rate_sum"] == 0:
            channel_sum_db[index]["success_rate"] = 0
        else:
            channel_sum_db[index]["success_rate"] *= 100
            channel_sum_db[index]["success_rate"] /= channel_sum_db[index]["success_rate_sum"]
        del channel_sum_db[index]["success_rate_sum"]
        # } liupan modify, 2018/6/1
        del channel_sum_db[index]["freeze_rate_sum"]
        # } liupan modify, 2018/5/31
        if channel_sum_db[index]["bit_time"] == 0:
            channel_sum_db[index]["bitrate"] = 0
        else:
            channel_sum_db[index]["bitrate"] = channel_sum_db[index]["bit_sum"] / \
                channel_sum_db[index]["bit_time"]

        # add by qjk
        # 增加对平均卡顿间隔的统计
        cal_avg('freeze_avg_n', 'freeze_avg_iv', channel_sum_db[index], True)
        # 增加对平均延时的统计
        cal_avg('delayed_avg_n', 'delayed_avg', channel_sum_db[index], True)

        # 插入数据库
        channel_sum_in.insert_one(channel_sum_db[index])
    logger.info(
        'channel_sum_in get count =%d p_count=%d' %
        (get_count, channel_sum_count))

    ############################
    ## 合并history user by node数据 ##
    ############################
    get_count = 0
    # 记录合并之后的数据
    history_user_by_node_db = []
    # 记录每种类型的合并条数，用于求平均值
    all_count_hs = []
    try:
        while get_count < history_user_by_node_count:
            if get_count == 0:
                # 从队列中取数据
                history_user_by_node_db.append(queue_history_user_by_node.get(True, 30))
                all_count_hs.append(1)
                get_count += 1
            else:
                # 从队列中取数据
                temp_db = queue_history_user_by_node.get(True, 30)
                get_count += 1
                index = -1
                count = 0
                # 查找当前是否有该类型的数据
                for history_user_by_node_x in history_user_by_node_db:
                    if history_user_by_node_x["cdn"] == temp_db["cdn"] and history_user_by_node_x["s_region"] == temp_db[
                        "s_region"] and history_user_by_node_x["s_operator"] == temp_db["s_operator"] and history_user_by_node_x[
                        "u_region"] == temp_db["u_region"] and history_user_by_node_x["u_operator"] == temp_db["u_operator"]:
                        index = count
                        break
                    count += 1
                # 没有该类型的数据，直接加入
                if index == -1:
                    history_user_by_node_db.append(temp_db)
                    all_count_hs.append(1)
                # 有该类型的数据，进行合并
                else:
                    all_count_hs[index] += 1
                    # 由于统计时的代码已经写死了这些key名，所以使用前不用判断key是否存在。
                    history_user_by_node_db[index]["freeze_rate"] += temp_db["freeze_rate"]
                    history_user_by_node_db[index]["freeze_rate_sum"] += temp_db["freeze_rate_sum"]
                    history_user_by_node_db[index]["bitrate"] += temp_db["bitrate"]
                    history_user_by_node_db[index]["success_rate"] += temp_db["success_rate"]
                    history_user_by_node_db[index]["success_rate_sum"] += temp_db["success_rate_sum"]
                    history_user_by_node_db[index]["band_width"] += temp_db["band_width"]
                    history_user_by_node_db[index]["bit_sum"] += temp_db["bit_sum"]
                    history_user_by_node_db[index]["bit_time"] += temp_db["bit_time"]
                    history_user_by_node_db[index]["user_n"] += temp_db["user_n"]
                    history_user_by_node_db[index]["req_n"] += temp_db["req_n"]
                    history_user_by_node_db[index]["jam_all"] += temp_db["jam_all"]
                    history_user_by_node_db[index]["duration"] += temp_db["duration"]
                    # } liupan add, 2018/6/14

                    sum_data(
                        'delayed_avg',
                        'delayed_avg_n',
                        temp_db,
                        history_user_by_node_db[index])

        else:
            logger.info("get queue_history_user_by_node Ok!")
    except queue.Empty:
        logger.info("queue_history_user_by_node empty.")
    else:
        pass
    # 求平均，并插入数据库
    for index in range(len(history_user_by_node_db)):
        # { liupan modify, 2018/5/31
        # if all_count_hs[index] == 0:
        #     history_single_db[index]["freeze_rate"] = 0;
        #     history_single_db[index]["success_rate"] = 0;
        # else:
        #     history_single_db[index]["freeze_rate"] /= all_count_hs[index];
        #     history_single_db[index]["success_rate"] /= all_count_hs[index];
        if history_user_by_node_db[index]["freeze_rate_sum"] == 0:
            history_user_by_node_db[index]["freeze_rate"] = 0
            # { liupan add, 2018/6/14
            history_user_by_node_db[index]["ps_freeze_rate"] = 0
            # } liupan add, 2018/6/14
        else:
            history_user_by_node_db[index]["freeze_rate"] *= 100
            history_user_by_node_db[index]["freeze_rate"] /= history_user_by_node_db[index]["freeze_rate_sum"]
            # { liupan add, 2018/6/14
            history_user_by_node_db[index]["ps_freeze_rate"] *= 100
            history_user_by_node_db[index]["ps_freeze_rate"] /= history_user_by_node_db[index]["freeze_rate_sum"]
            # } liupan add, 2018/6/14
        # { liupan modify, 2018/6/1
        # history_single_db[index]["success_rate"] /= all_count_hs[index];
        if history_user_by_node_db[index]["success_rate_sum"] == 0:
            history_user_by_node_db[index]["success_rate"] = 0
        else:
            history_user_by_node_db[index]["success_rate"] *= 100
            history_user_by_node_db[index]["success_rate"] /= history_user_by_node_db[index]["success_rate_sum"]
        del history_user_by_node_db[index]["success_rate_sum"]
        del history_user_by_node_db[index]["freeze_rate_sum"]
        if history_user_by_node_db[index]["bit_time"] == 0:
            history_user_by_node_db[index]["bitrate"] = 0
        else:
            history_user_by_node_db[index]["bitrate"] = history_user_by_node_db[index]["bit_sum"] / \
                                                        history_user_by_node_db[index]["bit_time"]

        # 增加对平均延时的统计
        cal_avg('delayed_avg_n', 'delayed_avg', history_user_by_node_db[index], True)

        # 插入数据库
        history_user_by_node.insert_one(history_user_by_node_db[index])
    logger.info(
        'history_user_by_node get count =%d p_count=%d' %
        (get_count, history_user_by_node_count))


    ################
    ## 关闭数据库 ##
    ################
    main_client.close()
# } liupan add, 2017/8/1


if __name__ == '__main__':
    LOG_FILE = 'user_stat.log'
    handler = logging.handlers.RotatingFileHandler(
        LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=5)  # 实例化handler
    fmt = '%(asctime)s - %(filename)s:%(lineno)s - %(name)s - %(message)s'
    formatter = logging.Formatter(fmt)   # 实例化formatter
    handler.setFormatter(formatter)      # 为handler添加formatter
    logger = logging.getLogger('user_stat')    # 获取名为tst的logger
    logger.addHandler(handler)           # 为logger添加handler
    logger.setLevel(logging.DEBUG)

    libip = cdll.LoadLibrary(os.getcwd() + '/libipdb.so')
    _init = libip.IPDB_Init
    _exit = libip.IPDB_Exit
    get_ip = libip.IPDB_Get_Region
    get_ip.argtypes = (c_int, c_char_p, c_char_p, c_char_p)
    _init()
    time_now = 0
    time_last = 0

    # { liupan add, 2017/7/13
    getVersionConfig()         # 获取版本信息
    # } liupan add, 2017/7/13

    # { liupan modify, 2017/7/13
    # if statis_version=='CNTV':
    #    subordinate_client = MongoClient("192.168.2.29", 27017)
    #    subordinate_db = subordinate_client.cntv_log_db
    # elif statis_version=='LD_KW':
    #    subordinate_client = MongoClient("10.26.167.116", 27017)
    #    subordinate_db = subordinate_client.ld_log_db
    # { liupan add, 2017/7/11
    # elif statis_version == 'JAPAN':
    #    subordinate_client = MongoClient("10.26.167.116", 27017);             # ???
    #    subordinate_db = subordinate_client.jp_log_db;
    # } liupan add, 2017/7/11
    # { liupan modify, 2018/4/24
    # subordinate_client = MongoClient(mongodb_subordinater_ip, mongodb_subordinater_port);
    if mongodb_user == '':
        subordinate_client = MongoClient(mongodb_subordinater_ip, mongodb_subordinater_port)
    else:
        subordinate_client = MongoClient(
            'mongodb://%s:%s@%s:%s/default_db?authSource=admin' %
            (mongodb_user, mongodb_pwd, mongodb_subordinater_ip, mongodb_subordinater_port))
    # } liupan modify, 2018/4/24
    subordinate_db = subordinate_client.get_database(mongodb_db)
    # } liupan modify, 2017/7/13

    logger.info("start get_user_stat.py.")
    while True:
        time_now = int(time.time())
        if time_now - time_last >= 60:
            logger.info("start Processing.")
            time_last = time_now
            start = int(time.time())
            struct_time = time.localtime(time.time())
            ip_dic = {}
            vip_list = []
            read_ip_dic_flag = 0

            # { liupan modify, 2017/8/1
            # # { liupan modify, 2017/7/11
            # if statis_version == 'CNTV' or statis_version == 'LD_KW':
            #     get_vip_node_list(vip_list);
            # # } liupan modify, 2017/7/11
            get_vip_node_list(vip_list)
            logger.info("get cdn list end %d." % read_ip_dic_flag)
            ip_dic = {}
            ip_dic = pickle.load(open("node_ip_list_dump", "rb"))
            collection_user = subordinate_db.ori_user
            # minutes=struct_time.tm_sec+180+(struct_time.tm_min%3)*60
            minutes = struct_time.tm_sec + 180
            start = start - minutes
            logger.info(start)
            if (start % 10) != 0:
                logger.info('time error:%d' % start)
                continue
            # { liupan add, 2018/6/14
            channel_freeze_dict = {}
            # } liupan add, 2018/6/14
            query = {"log_time": start}
            # query={"log_time":1492052400}
            res = {"_id": 0, "agent": 0}
            # find mongodb
            user_find_data = collection_user.find(query, res)
            find_time = int(time.time()) - time_now
            logger.info("find db time =%d" % find_time)
            # print(user_find_data.value());
            # print("len(user_find_data)");
            logger.info("user_find_data.count:%d" % user_find_data.count())
            thread_count = 0
            process_count = 0
            process_list = []
            result_input_list = []
            # { liupan modify, 2017/8/1
            # if statis_version=='CNTV':
            #     process_node_count=int(user_find_data.count()/16);
            # elif statis_version=='LD_KW':
            #     process_node_count=int(user_find_data.count()/4);
            # # { liupan add, 2017/7/11
            # elif statis_version == 'JAPAN':
            #     process_node_count = int(user_find_data.count() / 4);
            # # } liupan add, 2017/7/11
            process_node_count = int(user_find_data.count() / process_num)
            # } liupan modify, 2017/8/1
            #logger.info("process count:",process_node_count);
            create_flag = 0
            # { liupan modify, 2017/8/1
            # if statis_version=='CNTV':
            #     cdn_count={'kw':0,'dl':0,'ws':0,'qt':0,'pbs':0,'ctt':0};
            #     cdn_count_from={'kw':0,'dl':0,'ws':0,'qt':0,'pbs':0,'ctt':0,'none':0};
            #     cdn_test_detail=['none','kw','dl','ws','pbs','ctt','qt'];
            #     process_queue = multiprocessing.Queue(20)
            # elif statis_version=='LD_KW':
            #     cdn_count={'ld_kw':0,'qt':0};
            #     cdn_count_from={'ld_kw':0,'qt':0,'none':0};
            #     cdn_test_detail=['none','ld_kw'];
            #     process_queue = multiprocessing.Queue(10)
            # # { liupan add, 2017/7/11
            # elif statis_version == 'JAPAN':
            #     cdn_count = {'sdzx':0, 'qt':0};
            #     cdn_count_from = {'sdzx':0, 'qt':0, 'none':0};
            #     cdn_test_detail = ['none', 'sdzx'];
            #     process_queue = multiprocessing.Queue(10);
            # # } liupan add, 2017/7/11
            cdn_count = {}             # log使用
            cdn_count_from = {}        # log使用
            for name in cdn_all:
                cdn_count_from[name] = 0
                cdn_count[name] = 0
            cdn_test_detail = cdn_all       # log使用
            process_queue = multiprocessing.Queue(
                process_num)         # 多进程共享队列
            queue_statistic = multiprocessing.Queue()                  # 实时数据
            queue_history_single = multiprocessing.Queue()             # history single数据
            queue_history_sum = multiprocessing.Queue()                # history sum数据
            queue_channel_sum = multiprocessing.Queue()                # channel sum数据
            queue_history_user_by_node = multiprocessing.Queue()       # history_user_by_node数据
            # 记录各进程中history single的数据条数
            history_single_count = Array('i', process_num)  # []
            # 记录各进程中channel sum的数据条数
            channel_sum_count = Array('i', process_num)  # []
            # 记录各进程中history_user_by_node的数据条数
            history_user_by_node_count = Array('i', process_num)  # []
            # 初始化
            for i in range(process_num):
                # history_single_count.append(0);
                # channel_sum_count.append(0);
                history_single_count[i] = 0
                channel_sum_count[i] = 0
                history_user_by_node_count[i] = 0
            count_index = 0
            # } liupan modify, 2017/8/1
            # { liupan add, 2018/6/13
            ip_equal_dic = {}
            # } liupan add, 2018/6/13
            for result in user_find_data:
                str_node_ip = result['s_ip']
                if str_node_ip in ip_dic:
                    cdn_temp = ip_dic[str_node_ip][3].strip()
                    cdn_count[cdn_temp] += 1
                n_cdn_temp = result['from']
                # { liupan modify, 2017/9/29
                # str_cdn=cdn_test_detail[n_cdn_temp];
                # cdn_count_from[str_cdn]+=1;
                if (n_cdn_temp < len(cdn_test_detail)):
                    str_cdn = cdn_test_detail[n_cdn_temp]
                    cdn_count_from[str_cdn] += 1
                # } liupan modify, 2017/9/29
                # create process
                # { liupan add, 2018/6/13
                if str_node_ip not in ip_equal_dic:
                    ip_equal_dic[str_node_ip] = 1
                    result_input_list.append(result)
                # } liupan add, 2018/6/13
                if thread_count < process_node_count:
                    thread_count += 1
                    # { liupan delete, 2018/6/13
                    # result_input_list.append(result);
                    # } liupan delete, 2018/6/13
                    create_flag = 0
                else:
                    thread_count = 0
                    # { liupan delete, 2018/6/13
                    # result_input_list.append(result);
                    # } liupan delete, 2018/6/13
                    # { liupan modify, 2017/8/1
                    # process=multiprocessing.Process(target=nuser_stat_process,args=(result_input_list,ip_dic,vip_list,process_queue));
                    process = multiprocessing.Process(
                        target=nuser_stat_process,
                        args=(
                            result_input_list,
                            ip_dic,
                            vip_list,
                            process_queue,
                            queue_statistic,
                            queue_history_single,
                            queue_history_sum,
                            queue_channel_sum,
                            queue_history_user_by_node,
                            history_single_count,
                            channel_sum_count,
                            history_user_by_node_count,
                            count_index))
                    count_index += 1
                    # } liupan modify, 2017/8/1
                    process_list.append(process)
                    process_count += 1
                    result_input_list = []
                    create_flag = 1
            # if lastest process dont created.create lastest process.
            if create_flag == 0:
                # { liupan modify, 2017/8/1
                # process=multiprocessing.Process(target=nuser_stat_process,args=(result_input_list,ip_dic,vip_list,process_queue));
                process = multiprocessing.Process(
                    target=nuser_stat_process,
                    args=(
                        result_input_list,
                        ip_dic,
                        vip_list,
                        process_queue,
                        queue_statistic,
                        queue_history_single,
                        queue_history_sum,
                        queue_channel_sum,
                        queue_history_user_by_node,
                        history_single_count,
                        channel_sum_count,
                        history_user_by_node_count,
                        count_index))
                count_index += 1
                # } liupan modify, 2017/8/1
                process_list.append(process)
                process_count += 1
            devide_time = int(time.time()) - time_now
            logger.info(
                "devide time=%d process_count= %d" %
                (devide_time, process_count))
            for p in process_list:
                p.daemon = True
                p.start()
            find_time = int(time.time()) - time_now - devide_time
            logger.info("start process time=%d" % find_time)
            i_test = 0
            # for p in process_list:
            #    while p.is_alive():
            #        time.sleep(3);
            #        logger.info("join %d sleep."%i_test);
            #    else:
            #        logger.info("join %d success."%i_test);
            #        i_test+=1
            stat_time = int(time.time()) - time_now - find_time - devide_time
            logger.info("Process count=%d" % process_count)
            #logger.info("stat_time time =%d"%stat_time);
            logger.info("Process Ending %d " % stat_time)
            push_into_mongo(process_queue, process_count, start, process_list)
            # { liupan add, 2017/8/1
            # 当函数push_into_mongo执行完成之后，说明所有的进程已经执行完成了。
            # 因为push_into_mongo会阻塞读取共享队列process_queue中的数据，当读取完成，说明所有进程已执行完成。
            # 合并各进程的statistic、history_single、history_sum、channel_sum数据，并将合并后的数据插入数据库中。
            push_into_mongo2(
                queue_statistic,
                queue_history_single,
                queue_history_sum,
                queue_channel_sum,
                queue_history_user_by_node,
                process_count,
                history_single_count,
                channel_sum_count,
                history_user_by_node_count)
            i_test = 0
            for p in process_list:
                while p.is_alive():
                    time.sleep(1)
                    logger.info("join %d sleep." % i_test)
                else:
                    logger.info("join %d success." % i_test)
                    i_test += 1
            # } liupan add, 2017/8/1
            logger.info(cdn_count)
            logger.info(cdn_count_from)
            logger.info(start)
            logger.info("push_into_mongo End")
            time.sleep(2)
        else:
            time.sleep(1)
    _exit()
    subordinate_client.close()
    # client.close();
