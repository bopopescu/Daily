#!/usr/bin/env python
# -*- coding: utf-8 -*-
from aip import AipSpeech
import os
import sys

""" 你的 APPID AK SK """
APP_ID = '14831884'
API_KEY = 'DpC0KF6juALe8WBgEgGT6Bop'
SECRET_KEY = ''

client = AipSpeech(APP_ID, API_KEY, SECRET_KEY)

file_path = "C:\\Users\\XU-ZJ\\Desktop\\temporary\\16k.wav"
# 读取文件
def get_file_content(filePath):
    with open(filePath, 'rb') as fp:
        return fp.read()

# 识别本地文件
result = client.asr(get_file_content(file_path), 'wav', 16000, {
    'dev_pid': 1536,
})
print(result)