#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import re
import requests
from bs4 import BeautifulSoup

def gethtml(url, headers={}):
    req = urllib.request.Request(url, headers=headers)
    response = urllib.request.urlopen(req)
    content = response.read().decode('utf-8')
    response.close()
    print(content)
    return content

def get_albumn(urls):
    headers = {
        "Host": "http://music.163.com/#/discover/artist/cat?id=1001",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.119 Safari/537.36",
        "Cookie":"vjuids=2999910a4.15c690a5866.0.389051ec1c8c3; "
                 "_ntes_nnid=25bd77356e79e7f67c14ca242bb1d4f1,1496410904693; "
                 "_ntes_nuid=25bd77356e79e7f67c14ca242bb1d4f1; "
                 "usertrack=ZUcIhlk/2aQz6AtXA1B6Ag==; "
                 "ui_tip_cookie=xu18602751429%261%261%260%7C; "
                 "__utma=187553192.353896618.1500998294.1502178648.1502178648.1; "
                 "__utma=205185811.865861045.1500879760.1500879760.1502178579.2; "
                 "mail_psc_fingerprint=7eaf663c874a490e07c70cd62241efe3; "
                 "__utma=66909992.353896618.1500998294.1502178559.1504105936.2; "
                 "T_INFO=51AF4CB11571A34AEA08839A7BAB8C8B; c98xpt_=30; "
                 "l_yd_sign=-0201705313tZVMUrk1o7ETvSJR334wyCkC5gE_2Lyu9O8Hkeo30MGQMyg24iLMHAVHdnnkjuiJVoY2RpeB1MxtspY53Xo24GqfuOeANxcmh7oYL0Nrc1g..; "
                 "KAOLA_ACC=yd.4866b5e7d44542418@163.com; _ga=GA1.2.353896618.1500998294; "
                 "vjlast=1496410905.1516371715.21; vinfo_n_f_l_n3=445854a534e6d306.1.7.1496410911427.1512651485400.1516371876097; "
                 "nts_mail_user=18811727193@163.com:-1:1; P_INFO=m18811727193@163.com|1520865268|0|mail163|00&99|bej&1520851437&mail163#bej&null#10#0#0|188193&1|163&mail163|18811727193@163.com; "
                 "jsessionid-cpta=3n5Htm%2F1A79AUXcY4Q9gq5PhvGA%2BpoE%2BozJ0%2FBX4Zr6PeN9gl8wchoSu7ClPy58O6xjYtvL5%2BavxTeCKTwf3PLUD%2BQBnA9trPyOrBHw2KFROyHS9ceRo12QNynikyBONxJ%2BGk3WKAOrILDvbR1G6ZDimw1ct3M2VvdBfVCZgCmCXNOii%3A1525708772296"}

    response = requests.get(urls,headers=headers)
    return response.text

def parsehtml(html):
    soup = BeautifulSoup(html,"html.parser")
    list_art_id = soup.select("ul#m-artist-box li p href")
    print(list_art_id)

if __name__=="__main__":
    url = "http://music.163.com/#/discover/artist/cat?id=1001"
    url = gethtml(url, headers={
        'User-Agent': 'Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)',
        'Host': 'http://music.163.com/#/discover/artist/cat?id=1001'
    })
    parsehtml(html)