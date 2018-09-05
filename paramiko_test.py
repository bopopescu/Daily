#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
使用paramiko模块实现SSH功能
"""
import paramiko

# 创建SSH对象
ssh = paramiko.SSHClient()
# 允许连接不在known_hosts文件上的主机
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
# 连接服务器
ssh.connect(hostname="", port=22, username="root", password="")
# 执行命令
stdin, stdout, stderr = ssh.exec_command('cd /home/ubuntu/code/test;cat test.conf')
# 获取结果
result = stdout.read().decode()
# 获取错误提示（stdout、stderr只会输出其中一个）
err = stderr.read()
# 关闭连接
ssh.close()
print(stdin, result, err)


#实现SFTP功能
# 连接虚拟机centos上的ip及端口
transport = paramiko.Transport(("39.106.220.85", 22))
transport.connect(username="", password="")
# 将实例化的Transport作为参数传入SFTPClient中
sftp = paramiko.SFTPClient.from_transport(transport)
# 将“calculator.py”上传到filelist文件夹中
sftp.put('D:\Project\Daily\other.txt', '/home/ubuntu/code/test')
# 将centos中的aaa.txt文件下载到桌面
sftp.get('/home/ubuntu/code/test/test.conf', r'C:\Users\XU-ZJ\Desktop\test.conf')
transport.close()
