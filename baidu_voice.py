#!/usr/bin/env python
# -*- coding: utf-8 -*-
from aip import AipSpeech
from pydub import AudioSegment
import wave

import numpy as np
import subprocess
import select
import time
import os
import sys
# """ 你的 APPID AK SK """
APP_ID = '14831884'
API_KEY = 'DpC0KF6juALe8WBgEgGT6Bop'
SECRET_KEY = 'DqwmOpbS6qzSrS1uBn8uR7pNpXgP46D0'

step = 500 * 1000
CutTimeDef = 60
# client = AipSpeech(APP_ID, API_KEY, SECRET_KEY)
#
file_path = "D:\\JIE\\code\\Daily-master\\Daily-master\\testall.wav"


def CutFile():
    print("CutFile File Name is ", file_path)
    f = wave.open(file_path, "rb")
    params = f.getparams()
    print(params)
    nchannels, sampwidth, framerate, nframes = params[:4]
    CutFrameNum = framerate * CutTimeDef
    # 读取格式信息
    # 一次性返回所有的WAV文件的格式信息，它返回的是一个组元(tuple)：声道数, 量化位数（byte单位）, 采样频率
    # 采样点数, 压缩类型, 压缩类型的描述。wave模块只支持非压缩的数据，因此可以忽略最后两个信息

    print("CutFrameNum=%d" % (CutFrameNum))
    print("nchannels=%d" % (nchannels))
    print("sampwidth=%d" % (sampwidth))
    print("framerate=%d" % (framerate))
    print("nframes=%d" % (nframes))
    str_data = f.readframes(nframes)
    f.close()  # 将波形数据转换成数组
    # Cutnum =nframes/framerate/CutTimeDef
    # 需要根据声道数和量化单位，将读取的二进制数据转换为一个可以计算的数组
    wave_data = np.fromstring(str_data, dtype=np.short)
    wave_data.shape = -1, 2
    wave_data = wave_data.T
    temp_data = wave_data.T
    StepNum = CutFrameNum
    StepTotalNum = 0
    haha = 0
    while StepTotalNum < nframes:
        # for j in range(int(Cutnum)):
        print("Stemp=%d" % (haha))
        FileName = str(haha + 1) + ".wav"
        print(FileName)
        temp_dataTemp = temp_data[StepNum * (haha):StepNum * (haha + 1)]
        haha = haha + 1
        StepTotalNum = haha * StepNum
        temp_dataTemp.shape = 1, -1
        temp_dataTemp = temp_dataTemp.astype(np.short)  # 打开WAV文档
        f = wave.open(FileName, "wb")  #
        # 配置声道数、量化位数和取样频率
        f.setnchannels(nchannels)
        f.setsampwidth(sampwidth)
        f.setframerate(framerate)
        # 将wav_data转换为二进制数据写入文件
        f.writeframes(temp_dataTemp.tostring())
        f.close()



def sec_convert(seconds, loop=False):
    """秒数转换成时长(时/分/秒)"""
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    if loop:
        return "%02d_%02d_%02d" % (h, m, s)
    return "%02d:%02d:%02d" % (h, m, s)


if __name__ == '__main__':
    CutFile()
    print("Run Over")
# # 读取文件
#
# def get_file_content(filePath):
#     with open(filePath, 'rb') as fp:
#         return fp.read()
#
# # 识别本地文件
# result = client.asr(get_file_content(file_path), 'pcm', 16000, {
#     'dev_pid': 1537
# })
# print(result)


# class SpeechRecognition(object):
#
#     def __init__(self, app_id, api_key, secret_key):
#         self.app_id = app_id
#         self.api_key = api_key
#         self.secret_key = secret_key
#         try:
#             self.client = AipSpeech(self.app_id, self.api_key, self.secret_key)
#         except Exception as e:
#             raise Exception("Baidu App_id verify failed, checkout your secret_key error: %s", e)
#
#     @staticmethod
#     def get_file_content(file):
#         with open(file, 'rb') as fp:
#             return fp.read()
#
#     def get_result(self, f_path):
#         return self.client.asr(self.get_file_content(f_path), 'pcm', 16000, {
#             'dev_pid': 1537
#         })
#
#
# speech = SpeechRecognition(APP_ID, API_KEY, SECRET_KEY)
# res = speech.get_result(file_path)
# print(res)
# song = AudioSegment.from_file("testall.wav", frame_rate=16000, channels=1, sample_width=2)
# print(len(song))
# for index in range(0, len(song), step):
#     print(index)
#     index = index / 1000
#     step = step / 1000
#     print(sec_convert(index+step))
#     part_one = song[index:(index+step)]
#     part_one.export((sec_convert(index+step, loop=True) + ".wav"), format="wav")
# ten_seconds = 50 * 1000
# part1 = song[:ten_seconds]
# part1.export("part1.wav", format="wav")

#
# if __name__ == "__main__":
#     ori_video = sys.argv[1]
