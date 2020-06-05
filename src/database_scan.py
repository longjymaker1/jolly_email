#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/8/30 17:51
# @Author  : Long
# @Site    : 
# @File    : database_scan.py
# @Software: PyCharm
from src.SQL_base import run_sql, run_execute


def get_database_list():
    """创建databases列表"""
    database_lst_sql = """show databases;"""
    lst_tmp = run_sql(sqlmsg=database_lst_sql, is_to_pd=False)
    return [v[0] for v in lst_tmp]


def get_tabales_lst(database_name):
    """
    获取数据库对应表的列表
    :param database_name: 数据库名
    :return: 数据库名, 表列表
    """
    use_tabale = """
    use {database};
    """.format(database=database_name)
    run_execute(use_tabale)

    table_lst_sql = """
    show tables ;
    """
    # print(table_lst_sql)
    lst_tmp = run_sql(sqlmsg=table_lst_sql, is_to_pd=False)
    return lst_tmp


def get_table_msg(database_name, table_name):
    """
    获取表信息
    :param database_name: 数据库名称
    :param table_name: 表名称
    :return: 表信息
    """
    print(database_name, table_name)
    table_msg_sql = """
    DESCRIBE {table};
    """.format(table=table_name)
    lst_tmp = run_sql(sqlmsg=table_msg_sql, is_to_pd=False)
    return lst_tmp


def create_database_dict():
    """
    根据获取数据库结构信息创建字典
    :return: {database_name: [(table_name1, table_mst_object),(table_name2, table_mst_object)]}
    """
    databases_trc = {}
    database_lst = get_database_list()
    for db in database_lst:
        table_lst = get_tabales_lst(db)
        databases_trc[db] = []
        for tb in table_lst:
            t0 = (tb[0], table_msg(get_table_msg(db, tb[0])))
            databases_trc[db].append(t0)
    return databases_trc


class table_msg:
    def __init__(self, table_msg_lst):
        self.table_msg = table_msg_lst


if __name__ == '__main__':
    # lst = get_table_msg("atplanflight", "flight_notice_records")
    lst = create_database_dict()
    print(lst)