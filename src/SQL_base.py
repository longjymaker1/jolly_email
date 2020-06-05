#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/7/29 14:30
# @Author  : Long
# @Site    :
# @File    : SQL_base.py
# @Software: PyCharm
"""
读取sql文件, 运行sql并返回结果
"""
# from src import sql_file
import pandas as pd
from impala.dbapi import connect


def connect_impala():
    conn = connect(
        # host='172.31.2.214',
        # host='10.151.0.14',
        host="172.31.26.8",
        port=25000,
        user='long',
        password='long123'
                   )
    curr = conn.cursor(user='long')
    return conn, curr


def run_execute(sqlmsg: str):
    """运行不需要返回数据的sql"""
    conn, cur = connect_impala()
    cur.execute(sqlmsg)
    cur.close()
    conn.close()


def run_sql(sqlmsg: str, is_return=True, is_to_pd=True):
    """
    运行sql预计并返回结果
    :param is_return: 是否要返回值
    :param sqlmsg: sql语句
    :param is_to_pd: 是否转换为pandas.DataFrame, 默认为True, 转换为pandas.DataFrame
    :return: data
    """
    conn, cur = connect_impala()
    if sqlmsg is None:
        raise Exception("语句为空!!!")
    if is_return:
        print("读取数据--SQL")
        cur.execute(sqlmsg)
        data = cur.fetchall()
        cols = cur.description
        cur.close()
        conn.close()
        if is_to_pd:
            pd_data = pd.DataFrame(data, columns=[i[0] for i in cols])
            print("数据库读取数据完成")
            return pd_data
        else:
            print("数据库读取数据完成")
            return data
    else:
        print("执行语句")
        cur.execute(sqlmsg)
        cur.close()
        conn.close()
        print("语句执行完成")


