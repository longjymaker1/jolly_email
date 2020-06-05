#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/9/12 18:55
# @Author  : Long
# @Site    : 
# @File    : translation_email.py
# @Software: PyCharm
import os
import time
from src.email_base import emailSend

path = 'F:\pro\JollychicEmail\excel_file\\'


def get_file_name_list(paths=path):
    """获取指定路径中文件列表"""
    return [f for f in os.walk(paths)]


def send_email(file_lst):
    """判断文件是否在文件路径中，然后发送邮件"""
    main_file_tmp = '翻译_'
    main_file = ''
    for f in file_lst[0][2]:
        if main_file_tmp in f:
            main_file = f
    print(main_file)
    if main_file != '':
        file_path = path + main_file
        my_sender = emailSend(users=['long.long@jollycorp.com'], title=main_file, file_path=file_path)
        my_sender.sender()
        return 0


def hanld():
    flag = True
    while flag:
        lst = get_file_name_list()
        n = send_email(lst)
        if n == 0:
            flag = False
        print('等待文件创建')
        time.sleep(3600)


if __name__ == '__main__':
    hanld()