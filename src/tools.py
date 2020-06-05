#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/12/20 10:22
# @Author  : Long
# @Site    : 
# @File    : tools.py
# @Software: PyCharm

from functools import wraps
import pandas as pd
import datetime
import logging
import traceback
from logging.handlers import TimedRotatingFileHandler
import os


today = datetime.datetime.today()
father_dirname = os.path.dirname(os.path.dirname(__file__)) + '/'


def write_excels_one_sheet(filename: str, arrangement: list, sheet_name='data', **df_kwargs):
    """
    一个活多个DataFrame写入到同一个sheet中
    :param sheet_name: sheetname
    :param arrangement: 排列方式 [行, 列], 如果是多个df要写到一个sheet中，要按照n行，m列的方式写入
    :param filename: 文件名称，不含尾缀
    :param df_kwargs: 接收一个或多个DataFrame_name和DataFrame的键值对
    :return:
    """
    ns = 1
    for i in arrangement:
        ns *= i
    if ns < len(df_kwargs):
        raise TypeError("df数量大于行列数")
    str_today = datetime.datetime.strftime(today, '%Y%m%d')
    filename_lst = [filename + '_', str_today, '.xlsx']
    file_name = ''.join(filename_lst)
    file_path = os.path.join(father_dirname, 'excel_file/', file_name)

    excel_writer = pd.ExcelWriter(file_path, engine='xlsxwriter')
    row_num = 0
    col_num = 0
    row_lst = 0
    col_lst = 1
    max_item_len = 0
    for name, item in df_kwargs.items():
        item.to_excel(excel_writer, sheet_name, startrow=row_num+2, startcol=col_num, index_label=name)
        if len(item) >= max_item_len:
            max_item_len = len(item)
        col_lst += 1
        col_num += len(item.columns)+2
        if col_lst > arrangement[1]:
            row_lst += 1
            row_num += max_item_len+2
            max_item_len = 0
            col_num = 0
            col_lst = 1
    excel_writer.save()
    return file_path


def logger_in_file(filename: str):
    def decorator(func):
        def inner(*args, **kwargs):
            file_path = os.path.join(father_dirname, 'logs/', filename)
            log_file_path = file_path
            logger = logging.getLogger()
            logger.setLevel(logging.INFO)
            handler = TimedRotatingFileHandler(log_file_path, when='d', interval=1, backupCount=7)
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            try:
                result = func(*args, **kwargs)
                logger.info(result)
            except:
                msg = "func_name: {func_name}\r{msg}".format(func_name=func.__name__, msg=traceback.format_exc())
                logger.error(msg)
        return inner
    return decorator


@logger_in_file('newlog.log')
def test(n):
    print(1000/n)


@logger_in_file('newlog.log')
def test2(n):
    raise TypeError("error test")


if __name__ == '__main__':
    # tmp1 = pd.DataFrame([[1, 2, 3], [4, 5, 6]], columns=['a', 'b', 'c'])
    # tmp2 = pd.DataFrame([[7, 2, 3], [4, 33, 6]], columns=['a', 'b', 'c'])
    # tmp3 = pd.DataFrame([[8, 5, 56], [4, 5, 6]], columns=['a', 'b', 'c'])
    # tmp4 = pd.DataFrame([[32, 22, 2], [64, 5, 6]], columns=['a', 'b', 'c'])
    # tmp5 = pd.DataFrame([[123, 44, 3], [12, 44, 6]], columns=['a', 'b', 'c'])
    # write_excels_one_sheet('tmp', [3, 2], t1=tmp1, t2=tmp2, t3=tmp3, t4=tmp4, t5=tmp5)
    print(father_dirname)