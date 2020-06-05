#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020/1/15 16:02
# @Author  : Long
# @Site    : 
# @File    : saorao.py
# @Software: PyCharm
import os
import time


def shutdown():
    print("赶快保存文件，30秒后自动关机")
    time.sleep(1.5)
    os.system('shutdown -s -f -t 28')


if __name__ == '__main__':
    shutdown()