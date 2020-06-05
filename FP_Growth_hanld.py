#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020/3/12 10:52
# @Author  : Long
# @Site    : 
# @File    : FP_Growth.py
# @Software: vscode

from src import sql_file
from src.SQL_base import run_sql
from src.FP_Growth import loadSimpDat, createInitSet, mineTree, createTree, evaluation, time_cal, data_save
import time


def fp_growth_hanld(min_supp_num, min_conf):
    # min_supp_num = 5
    # min_conf = 0.30
    min_supp_num = min_supp_num
    min_conf = min_conf

    t0 = time.time()
    # data0 = loadSimpDat()
    # print(data0)
    # data0 = loadSimpDat(r'F:\pro\JollychicEmail\excel_file\association.xlsx')

    data0 = loadSimpDat(df=run_sql(sql_file.fg_group_sql(begin='20191001', end='20200311')))
    t1 = time.time()
    initSet = createInitSet(data0)
    t2 = time.time()
    myFPtree, myHeaderTab = createTree(initSet, min_supp_num)
    t3 = time.time()
    freqItemList = []
    mineTree(myFPtree, myHeaderTab, min_supp_num, set([]), freqItemList)
    t4 = time.time()
    freqItemDict = evaluation(freqItemList, min_conf=min_conf,
                              header_table=myHeaderTab, dataSet=data0)
    t5 = time.time()
    data_save(freqItemDict, r'F:\pro\JollychicEmail\excel_file\test.txt')
    t6 = time.time()

    print(freqItemDict)
    print("满足置信度的频繁项集个数, ", len(freqItemDict))
    h, m, s = time_cal(t0, t1)
    print("获取数据耗时  {h}h-{m}-m-{s}s".format(h=h, m=m, s=s))
    h, m, s = time_cal(t1, t2)
    print("数据转换  {h}h-{m}m-{s}s".format(h=h, m=m, s=s))
    h, m, s = time_cal(t2, t3)
    print("创建FP_tree, headerTable耗时  {h}h-{m}m-{s}s".format(h=h, m=m, s=s))
    h, m, s = time_cal(t3, t4)
    print("创建计算频繁项集耗时  {h}h-{m}m-{s}s".format(h=h, m=m, s=s))
    h, m, s = time_cal(t4, t5)
    print("验证频繁项集耗时  {h}h-{m}m-{s}s".format(h=h, m=m, s=s))
    h, m, s = time_cal(t5, t6)
    print("写入文档耗时  {h}h-{m}m-{s}s".format(h=h, m=m, s=s))
    h, m, s = time_cal(t0, t5)
    print("总耗时  {h}h-{m}m-{s}s".format(h=h, m=m, s=s))


if __name__ == "__main__":
    fp_growth_hanld(5, 0.30)