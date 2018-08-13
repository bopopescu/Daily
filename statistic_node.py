#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pymongo import MongoClient
import time
import json
from ip_list import *
import logging
import logging.handlers
import pickle
import configparser


# { liupan add, 2017/8/1
region_all = []
operator_all = []
cdn_all = []
bitrate_detail = {}

mongodb_master_ip = ''
mongodb_master_port = 27017
mongodb_slaver_ip = ''
mongodb_slaver_port = 27017
mongodb_db = ''
# { liupan add, 2018/4/24
mongodb_user = ''
mongodb_pwd = ''
# } liupan add, 2018/4/24


def getVersionConfig():
    global statis_version
    global mongodb_master_ip
    global mongodb_master_port
    global mongodb_slaver_ip
    global mongodb_slaver_port
    global mongodb_db
    global region_all
    global operator_all
    global cdn_all
    global mongodb_user
    global mongodb_pwd
    # } liupan add, 2018/4/24
    cp = configparser.ConfigParser()
    cp.read('version.conf')
    statis_version = cp.get('setting', 'version')
    mongodb_master_ip = cp.get('setting', 'mongodb_master_ip')
    mongodb_master_port = cp.getint('setting', 'mongodb_master_port')
    mongodb_slaver_ip = cp.get('setting', 'mongodb_slaver_ip')
    mongodb_slaver_port = cp.getint('setting', 'mongodb_slaver_port')
    mongodb_db = cp.get('setting', 'mongodb_db')
    mongodb_user = cp.get('setting', 'mongodb_user')
    mongodb_pwd = cp.get('setting', 'mongodb_pwd')
    region_all = cp.get('setting', 'regions').split(',')
    region_all.append('其他')
    operator_all = cp.get('setting', 'operators').split(',')
    operator_all.append('其他')
    cdn_all.append('qt')
    cdn_all += cp.get('setting', 'cdns').split(',')


def read_bitrate_config_common():
    cp = configparser.ConfigParser()
    cp.read('get_user_stat.conf')
    bitrate_list = {}
    for key, value in cp.items('bitrate'):
        bitrate_list[key] = int(value)

    return bitrate_list


def get_rate_n_detail_common(detail):
    bitrate_temp = 0
    bitrate_time = 0

    for key, value in detail.items():
        # { liupan modify, 2017/10/31
        # # { liupan add, 2017/9/1
        # if key not in bitrate_detail:
        #     bitrate_detail[key] = int(key);
        # # } liupan add, 2017/9/1
        # bitrate_temp += bitrate_detail[key] * int(value);
        if key not in bitrate_detail:
            bitrate_temp += int(key) * int(value)
        else:
            bitrate_temp += bitrate_detail[key] * int(value)
        # } liupan modify, 2017/10/31
        bitrate_time += int(value)

    ret_detail = {'bit_sum': bitrate_temp, 'bit_time': bitrate_time}
    return ret_detail
# } liupan add, 2017/8/1


def read_bitrate_config():
    cp = configparser.ConfigParser()
    cp.read('get_user_stat.conf')
    conf_list = []
    conf_list.append(cp.getint('bitrate', 'pd'))
    conf_list.append(cp.getint('bitrate', 'td'))
    conf_list.append(cp.getint('bitrate', 'ud'))
    conf_list.append(cp.getint('bitrate', 'hd'))
    conf_list.append(cp.getint('bitrate', 'md'))
    return conf_list


def get_rate_n_detail(detail):
    bitrate_temp = 0
    bitrate_time = 0
    for key, value in detail.items():
        bitrate_temp += bitrate_detail[int(key)] * value
        bitrate_time += value
    ret_detail = {'bit_sum': bitrate_temp, 'bit_time': bitrate_time}
    return ret_detail


def get_rate_n_detail_ld(detail):
    bitrate_temp = 0
    bitrate_time = 0
    for key, value in detail.items():
        bitrate_temp += int(key) * value
        bitrate_time += value
    ret_detail = {'bit_sum': bitrate_temp, 'bit_time': bitrate_time}
    return ret_detail


def read_ld_bitrate_config():
    cp = configparser.ConfigParser()
    cp.read('get_user_stat.conf')
    cdn_file_dic = {}
    for key, value in cp.items('bitrate'):
        cdn_file_dic[int(key)] = int(value)
    return cdn_file_dic


if __name__ == '__main__':
    LOG_FILE = 'statistic_node.log'

    handler = logging.handlers.RotatingFileHandler(
        LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=5)  # 实例化handler
    fmt = '%(asctime)s - %(filename)s:%(lineno)s - %(name)s - %(message)s'

    formatter = logging.Formatter(fmt)   # 实例化formatter
    handler.setFormatter(formatter)      # 为handler添加formatter
    logger = logging.getLogger('node_stat')    # 获取名为tst的logger
    logger.addHandler(handler)           # 为logger添加handler
    logger.setLevel(logging.DEBUG)

    getVersionConfig()
    global bitrate_detail

    if mongodb_user == '':
        slave_client = MongoClient(mongodb_slaver_ip, mongodb_slaver_port)
    else:
        slave_client = MongoClient(
            'mongodb://%s:%s@%s:%s/default_db?authSource=admin' %
            (mongodb_user, mongodb_pwd, mongodb_slaver_ip, mongodb_slaver_port))
    # } liupan modify, 2018/4/24
    slave_db = slave_client.get_database(mongodb_db)
    # } liupan modify, 2017/7/13
    start = 1533571200
    end = 1533657600
    while start <= end:
        logger.info("start thread :%s", start)
        ip_dic = {}
        vip_list = []
        read_ip_dic_flag = 0
        # { liupan modify, 2017/8/1
        # # { liupan modify, 2017/7/11
        # if statis_version=='CNTV' or statis_version=='LD_KW':
        #     get_vip_node_list(vip_list);
        # # } liupan modify, 2017/7/11
        get_vip_node_list(vip_list)
        # } liupan modify, 2017/8/1
        ip_conf_read = read_ip_list_config()
        # { liupan modify, 2017/8/1
        # if statis_version=='CNTV':
        #     bitrate_detail=read_bitrate_config();
        # elif statis_version=='LD_KW':
        #     bitrate_detail=read_ld_bitrate_config()
        # # { liupan add, 2017/7/11
        # elif statis_version == 'JAPAN':
        #     bitrate_detail = read_ld_bitrate_config();
        # # } liupan add, 2017/7/11
        bitrate_detail = read_bitrate_config_common()
        # } liupan modify, 2017/8/1
        # { liupan add, 2018/2/12
        h5_list = []
        get_h5_node_list(h5_list)
        # } liupan add, 2018/2/12
        for cdn_name, cdn_fit_file in ip_conf_read.items():
            # { liupan modify, 2017/10/26
            # ret=get_cdn_ip_list(cdn_fit_file,ip_dic,cdn_name);
            ret = get_cdn_ip_list_safe(logger, cdn_fit_file, ip_dic, cdn_name)
            # } liupan modify, 2017/10/26
            if ret != 0:
                logger.info("Read %s ip list Error." % cdn_name)
                read_ip_dic_flag = 1
                break
        if read_ip_dic_flag == 1:
            ip_dic = pickle.load(open("node_ip_list_dump", "rb"))
            logger.info("load node_ip_list_dump.")
        else:
            pickle.dump(ip_dic, open("node_ip_list_dump", "wb"))
            logger.info("dump node_ip_list_dump.")
        logger.info("get cdn list end %d." % read_ip_dic_flag)
        collection_node = slave_db.ori_node
        query = {"start": start}
        res = {
            "_id": 0,
            "from": 1,
            "start": 1,
            "s_ip": 1,
            "band": 1,
            "suc_r": 1,
            "freeze_r": 1,
            "bitrate": 1,
            'rate_n': 1,
            'freeze_avg_iv': 1,
            'delayed_avg': 1
        }
        # { liupan add, 2018/6/12
        res['user_n'] = 1
        res['req_n'] = 1
        # } liupan add, 2018/6/12
        st_node = {"time": start, "node_statistics": []}
        all_data = {}
        all_count = {}
        # all_data["all"]={"freeze_rate":0,"bitrate":0,"success_rate":0,"band_width":0};
        # all_count["all"]={'sum':0,'center':0};
        for i in region_all:
            all_data[i] = {}
            all_count[i] = {}
            for j in cdn_all:
                all_data[i][j] = {}
                all_count[i][j] = {}
                for k in operator_all:
                    all_data[i][j][k] = {
                        "freeze_rate": 0,
                        "bitrate": 0,
                        "success_rate": 0,
                        "band_width": 0,
                        'bit_sum': 0,
                        'bit_time': 0}
                    # { liupan add, 2018/2/12
                    all_data[i][j][k]['h5_band_width'] = 0
                    # } liupan add, 2018/2/12
                    # { liupan add, 2018/6/12
                    all_data[i][j][k]['user_n'] = 0
                    all_data[i][j][k]['req_n'] = 0
                    # } liupan add, 2018/6/12
                    all_count[i][j][k] = {'sum': 0, 'center': 0}

                    # 1 增加字段
                    all_data[i][j][k]['freeze_avg_iv'] = 0
                    all_data[i][j][k]['delayed_avg'] = 0
                    all_count[i][j][k]['freeze_avg_n'] = 0
                    all_count[i][j][k]['delayed_avg_n'] = 0
        node_count = 0
        drop_count = 0
        pbs_count = 0
        ip_equal_dic = {}
        equal_ip_count = 0
        drop_band_width = 0
        cdn_count = {}
        for key in cdn_all:
            cdn_count[key] = 0
        edge_data = {'band_width': 0, 'cdn': {}}
        # } liupan modify, 2017/8/1
        logger.info("node_find_data.count:%d", collection_node.find(query, res).count())
        for result in collection_node.find(query, res):
            cdn_name = 'qt'
            if result['from'] >= 0 and result['from'] < len(cdn_all):
                # } liupan modify, 2018/4/16
                cdn_name = cdn_all[result['from']]
            # } liupan add, 2017/12/15
            node_count += 1
            node = {
                "node": [],
                "freeze_rate": 0,
                "bitrate": 0,
                "success_rate": 0,
                "band_width": 0,
                'bit_sum': 0,
                'bit_time': 0,
                'freeze_avg_iv': 0,
                'delayed_avg': 0}
            value = result["s_ip"]
            # if "band" in result and "suc_r" in result:
            # if result["band"]==0 and result["suc_r"]==0:
            #        continue;
            if value in ip_equal_dic:
                equal_ip_count += 1
                continue
            ip_equal_dic[value] = 1
            level_temp = 0
            region_temp = '其他'
            # { liupan modify, 2017/12/15
            # cdn_temp='qt';
            cdn_temp = cdn_name
            # } liupan modify, 2017/12/15
            operator_temp = '其他'
            level_temp = 5
            if value in ip_dic:
                operator_temp = ip_dic[value][1].strip()
                region_temp = get_str_region(ip_dic[value][2])
                level_temp = int(ip_dic[value][0])

            ip_info = {
                "ip": value,
                "region": region_temp,
                "operator": operator_temp}
            cdn_count[cdn_temp] += 1
            node["level"] = level_temp
            node["cdn"] = cdn_temp
            node["vip"] = 0
            if value in vip_list:
                node["vip"] = 1
            if level_temp == 5 or node["vip"] == 1:
                edge_data['band_width'] += result["band"]
                if cdn_temp not in edge_data['cdn']:
                    edge_data['cdn'][cdn_temp] = {'band_width': 0}
                edge_data['cdn'][cdn_temp]['band_width'] += result["band"]
            # } liupan modify, 2017/12/15
            node["node"].append(ip_info)
            addflag = 1
            # { liupan add, 2018/6/12
            all_data[region_temp][cdn_temp][operator_temp]["user_n"] += result["user_n"]
            all_data[region_temp][cdn_temp][operator_temp]["req_n"] += result["req_n"]
            # } liupan add, 2018/6/12
            if "band" in result:
                node["band_width"] = result["band"]
                all_data[region_temp][cdn_temp][operator_temp]["band_width"] += result["band"]
                # { liupan add, 2018/2/12
                if value in h5_list:
                    all_data[region_temp][cdn_temp][operator_temp]["h5_band_width"] += result["band"]
                # } liupan add, 2018/2/12
                if result["band"] == 0:
                    addflag = 0
            if "suc_r" in result:
                node["success_rate"] = result["suc_r"]
                if addflag == 1:
                    all_data[region_temp][cdn_temp][operator_temp]["success_rate"] += result["suc_r"]
            if "freeze_r" in result:
                node["freeze_rate"] = result["freeze_r"]
                if addflag == 1:
                    all_data[region_temp][cdn_temp][operator_temp]["freeze_rate"] += result["freeze_r"]
            if "bitrate" in result:
                node["bitrate"] = result["bitrate"]
                if addflag == 1:
                    all_data[region_temp][cdn_temp][operator_temp]["bitrate"] += result["bitrate"]
            if 'rate_n' in result:
                bit_temp = get_rate_n_detail_common(result['rate_n'])
                # } liupan modify, 2017/8/1
                # print(bit_temp);
                all_data[region_temp][cdn_temp][operator_temp]["bit_sum"] += bit_temp["bit_sum"]
                all_data[region_temp][cdn_temp][operator_temp]["bit_time"] += bit_temp["bit_time"]
                node["bit_sum"] = bit_temp["bit_sum"]
                node["bit_time"] = bit_temp["bit_time"]
            if addflag == 1:
                all_count[region_temp][cdn_temp][operator_temp]["sum"] += 1
                if (level_temp == 3 or level_temp ==
                        4) and node["vip"] == 0:
                    all_count[region_temp][cdn_temp][operator_temp]["center"] += 1
            else:
                drop_count += 1
            if len(node["node"]) != 0:
                st_node["node_statistics"].append(node)

            if 'freeze_avg_iv' in result:
                node['freeze_avg_iv'] = result['freeze_avg_iv']
                if result['freeze_avg_iv'] > 0:
                    all_data[region_temp][cdn_temp][operator_temp]['freeze_avg_iv'] += result["freeze_avg_iv"]
                    all_count[region_temp][cdn_temp][operator_temp]['freeze_avg_n'] += 1

            if 'delayed_avg' in result:
                node['delayed_avg'] = result['delayed_avg']
                if result['delayed_avg'] > 0:
                    all_data[region_temp][cdn_temp][operator_temp]['delayed_avg'] += result["delayed_avg"]
                    all_count[region_temp][cdn_temp][operator_temp]['delayed_avg_n'] += 1

        master_client = MongoClient(
                    'mongodb://%s:%s@%s:%s/default_db?authSource=admin' %
                    ('admin123', '123', '39.107.109.85', 27017))
        # } liupan modify, 2018/4/24
        master_db = master_client.get_database(mongodb_db)
        # } liupan modify, 2017/7/13

        node_in = master_db.statistic_node
        logger.info("inserting table ")
        node_in.insert_one(st_node)
        start += 60
        master_client.close()
    slave_client.close()