#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/9/12 16:55
# @Author  : Long
# @Site    : 
# @File    : thread_translation.py
# @Software: PyCharm
from src.SQL_base import run_sql
import order_translation
from src import sql_file
import threading
import pandas as pd
from src.data_conversion import data_to_excel


def df_cut(thread_num=4):
    datadf = run_sql(sql_file.order_content)
    df_len = len(datadf)
    cut_num = df_len//thread_num
    df_lst = [datadf[:cut_num], datadf[cut_num*thread_num-1:]]
    for i in range(1, thread_num-1):
        df_lst.append(datadf[cut_num*i: cut_num*(i+1)])
    return df_lst


def thread_run_translation(df_lst):
    t0 = threading.Thread(target=order_translation.order_translation(df_lst[0]), args=("t0",))
    t1 = threading.Thread(target=order_translation.order_translation(df_lst[1]), args=("t1",))
    t2 = threading.Thread(target=order_translation.order_translation(df_lst[2]), args=("t2",))
    t3 = threading.Thread(target=order_translation.order_translation(df_lst[3]), args=("t3",))
    df0 = t0.start()
    df1 = t1.start()
    df2 = t2.start()
    df3 = t3.start()
    lst = [df0, df1, df2, df3]
    df_out = pd.concat(lst)
    return df_out


if __name__ == '__main__':
    datadf = df_cut(4)
    data = thread_run_translation(datadf)
    datas = [{'翻译': [(data, 'data'), ]}]
    file_path = data_to_excel(filename='翻译', data_list=datas)
