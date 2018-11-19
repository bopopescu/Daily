#!/usr/bin/env python
# -*- coding: utf-8 -*-
from aip import AipSpeech
from pydub import AudioSegment
import subprocess
import select
import time
import os
# """ 你的 APPID AK SK """
APP_ID = '14831884'
API_KEY = 'DpC0KF6juALe8WBgEgGT6Bop'
SECRET_KEY = 'DqwmOpbS6qzSrS1uBn8uR7pNpXgP46D0'

client = AipSpeech(APP_ID, API_KEY, SECRET_KEY)

file_path = "D:\\code\\Daily\\cut55.pcm"
# 读取文件
def get_file_content(filePath):
    with open(filePath, 'rb') as fp:
        return fp.read()

# 识别本地文件
result = client.asr(get_file_content(file_path), 'pcm', 16000, {
    'dev_pid': 1537
})
print(result)

    # song = AudioSegment.from_file("testall.wav", frame_rate=16000, channels=1, sample_width=2)
# ten_seconds = 50 * 1000
# part1 = song[:ten_seconds]
# part1.export("part1.wav", format="wav")