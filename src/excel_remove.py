# -*- coding: UTF8 -*-
"""
遍历指定文件路径，删除该路径下创建时间超过n天的文件
"""

import os
import datetime
import time

base_file_path = r"F:\pro\JollychicEmail\excel_file"


def TimeStampToTime(timestamp):
    """把时间戳转化为时间: 1479264792 to 2016-11-16 10:53:12"""
    timeStruct = datetime.datetime.fromtimestamp(timestamp)
    # otherStyleTime = time.strftime("%Y-%m-%d %H:%M:%S", timeStruct)
    return timeStruct


def file_name(file_dir, file_last_name):
    L = []
    for root, dirs, files in os.walk(file_dir):
        for file in files:
            if os.path.splitext(file)[1] == file_last_name:
                L.append(os.path.join(root, file))
    return L


def get_file_ctime(file_name):
    """获取文件创建时间"""
    return os.path.getctime(file_name)


def remove_file(file_path, ndays, file_last_name):
    """
    删除文件, 指定删除file_path路径下(不含子文件夹),创建日期大于ndays，尾缀为file_last_name的文件
    ndays, 大于ndays的文件删除
    file_last_name, 文件尾缀
    """
    today = datetime.datetime.now()
    file_lst = file_name(file_path, file_last_name)
    for i in file_lst:
        ctime = get_file_ctime(i)
        ctime_d = TimeStampToTime(ctime)
        diff_days = (today - ctime_d).days
        if diff_days >= ndays:
            os.remove(i)


if __name__ == '__main__':
    remove_file(base_file_path, 15, '.png')
