#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/9/3 16:01
# @Author  : Long
# @Site    : 
# @File    : user_loss_resale.py
# @Software: PyCharm
from src.SQL_base import run_sql
from src import sql_file
import pandas as pd


def get_data(begin_data: str, end_data: str):
    """
    获取用户购买订单数据
    :param begin_data: 开始时间 -> 'yyyyMMdd'
    :param end_data: 截止时间 -> 'yyyyMMdd'
    :return: order_data
    """
    sql_msg = sql_file.user_loss_reslae_sql(begin_data, end_data)
    data0 = run_sql(sql_msg)
    return data0


if __name__ == '__main__':
    data0 = get_data('20190827', '20190901')
    print(data0)