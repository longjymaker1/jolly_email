#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/9/11 18:24
# @Author  : Long
# @Site    : 
# @File    : order_translation.py
# @Software: PyCharm
from src.msg_translation import get_translate
from src import sql_file
from src.SQL_base import run_sql
from src.data_conversion import data_to_excel
import time


def order_translation(data_df):
    gmv_df = data_df
    gmv_df['content_ch'] = None
    ns = 0
    for row in gmv_df.index:
        flag = True
        er_n = 0
        print(row)
        if gmv_df.loc[row, 'content_ch'] is None:
            while flag:
                translate_text = gmv_df.loc[row, 'content']
                print('原文: ', translate_text)
                results = get_translate(translate_text, 'zh-CN')
                try:
                    print("type(results)=", type(results))
                    if len(results) == 0:
                        er_n += 1
                        print("sleep-->{n}s".format(n=er_n))
                        if er_n >= 20:
                            flag = False
                            continue
                        else:
                            time.sleep(er_n)
                            continue
                    if len(results[0]) >= 1 and type(results[0]) == list:
                        result_translate = results[0][0]
                    if len(results[0]) >= 1 and type(results[0]) == str:
                        result_translate = results[0]
                    print('译文: ', result_translate)
                except Exception as e:
                    print(results)
                    print(e)
                    raise RuntimeError('error')
                gmv_df.loc[row, 'content_ch'] = result_translate
                flag = False
                
            if row % 1000 == 0:
                df_tmp = gmv_df[:row]
                save_data(df_tmp, ns)
                ns += 1
    return gmv_df


def save_data(datas, nums):
    """将数据保存到excel"""
    file_name = '童鞋翻译' + str(nums)
    datas = [{'童鞋翻译': [(datas, 'data'), ]}]
    data_to_excel(filename=file_name, data_list=datas)


if __name__ == '__main__':
    datadf = run_sql(sql_file.order_content)
    data = order_translation(datadf)
    datas = [{'童鞋翻译': [(data, 'data'), ]}]
    file_path = data_to_excel(filename='童鞋翻译', data_list=datas)
