#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/8/13 10:52
# @Author  : Long
# @Site    : 
# @File    : FP_Growth.py
# @Software: PyCharm

from src import sql_file
from src.SQL_base import run_sql
import pandas as pd
import numpy as np
import time
import json


class treeNode:
    def __init__(self, nameValue, numOccur, parentNode):
        self.name = nameValue
        self.count = numOccur
        self.nodeLink = None
        # needs to be updated
        self.parent = parentNode
        self.children = {}

    def inc(self, numOccur):
        """inc(对count变量增加给定值)
        """
        self.count += numOccur

    def disp(self, ind=1):
        """disp(用于将树以文本形式显示)
        """
        print('  '*ind, self.name, ' ', self.count)
        for child in self.children.values():
            child.disp(ind+1)


def updateHeader(nodeToTest, targetNode):
    """updateHeader(更新头指针，建立相同元素之间的关系，例如： 左边的r指向右边的r值，就是后出现的相同元素 指向 已经出现的元素)
    从头指针的nodeLink开始，一直沿着nodeLink直到到达链表末尾。这就是链表。
    性能：如果链表很长可能会遇到迭代调用的次数限制。
    Args:
        nodeToTest  满足minSup {所有的元素+(value, treeNode)}
        targetNode  Tree对象的子节点
    """
    # 建立相同元素之间的关系，例如： 左边的r指向右边的r值
    while nodeToTest.nodeLink is not None:
        nodeToTest = nodeToTest.nodeLink
    nodeToTest.nodeLink = targetNode


def updateTree(items, inTree, headerTable, count):
    """updateTree(更新FP-tree，第二次遍历)
    # 针对每一行的数据
    # 最大的key,  添加
    Args:
        items       满足minSup 排序后的元素key的数组（大到小的排序）
        inTree      空的Tree对象
        headerTable 满足minSup {所有的元素+(value, treeNode)}
        count       原数据集中每一组Kay出现的次数
    """
    # 取出 元素 出现次数最高的
    # 如果该元素在 inTree.children 这个字典中，就进行累加
    # 如果该元素不存在 就 inTree.children 字典中新增key，value为初始化的 treeNode 对象
    if items[0] in inTree.children:
        # 更新 最大元素，对应的 treeNode 对象的count进行叠加
        inTree.children[items[0]].inc(count)
    else:
        # 如果不存在子节点，我们为该inTree添加子节点
        inTree.children[items[0]] = treeNode(items[0], count, inTree)
        # 如果满足minSup的dist字典的value值第二位为null， 我们就设置该元素为 本节点对应的tree节点
        # 如果元素第二位不为null，我们就更新header节点
        if headerTable[items[0]][1] is None:
            # headerTable只记录第一次节点出现的位置
            headerTable[items[0]][1] = inTree.children[items[0]]
        else:
            # 本质上是修改headerTable的key对应的Tree，的nodeLink值
            updateHeader(headerTable[items[0]][1], inTree.children[items[0]])
    if len(items) > 1:
        # 递归的调用，在items[0]的基础上，添加item0[1]做子节点， count只要循环的进行累计加和而已，统计出节点的最后的统计值。
        updateTree(items[1:], inTree.children[items[0]], headerTable, count)


def createTree(dataSet, minSup=1):
    """createTree(生成FP-tree)
    Args:
        dataSet  dist{行：出现次数}的样本数据
        minSup   最小的支持度
    Returns:
        retTree  FP-tree
        headerTable 满足minSup {所有的元素+(value, treeNode)}
    """
    # 支持度>=minSup的dist{所有元素：出现的次数}
    headerTable = {}
    # 循环 dist{行：出现次数}的样本数据
    for trans in dataSet:
        # 对所有的行进行循环，得到行里面的所有元素
        # 统计每一行中，每个元素出现的总次数
        for item in trans:
            # 例如： {'ababa': 3}  count(a)=3+3+3=9   count(b)=3+3=6
            headerTable[item] = headerTable.get(item, 0) + dataSet[trans]
    # 删除 headerTable中，元素次数<最小支持度的元素
    headerTable_lst_tmp = list(headerTable.keys())
    for k in headerTable_lst_tmp:
        if headerTable[k] < minSup:
            del(headerTable[k])

    # 满足minSup: set(各元素集合)
    freqItemSet = set(headerTable.keys())
    # 如果不存在，直接返回None
    if len(freqItemSet) == 0:
        return None, None
    for k in headerTable:
        # 格式化： dist{元素key: [元素次数, None]}
        headerTable[k] = [headerTable[k], None]

    # create tree
    retTree = treeNode('Null Set', 1, None)
    # 循环 dist{行：出现次数}的样本数据
    for tranSet, count in dataSet.items():
        # print 'tranSet, count=', tranSet, count
        # localD = dist{元素key: 元素总出现次数}
        localD = {}
        for item in tranSet:
            # 判断是否在满足minSup的集合中
            if item in freqItemSet:
                # print 'headerTable[item][0]=', headerTable[item][0], headerTable[item]
                localD[item] = headerTable[item][0]
        # print 'localD=', localD
        if len(localD) > 0:
            # p=key,value; 所以是通过value值的大小，进行从大到小进行排序
            # orderedItems 表示取出元组的key值，也就是字母本身，但是字母本身是大到小的顺序
            orderedItems = [v[0] for v in sorted(localD.items(), key=lambda p: p[1], reverse=True)]
            # print 'orderedItems=', orderedItems, 'headerTable', headerTable, '\n\n\n'
            # 填充树，通过有序的orderedItems的第一位，进行顺序填充 第一层的子节点。
            updateTree(orderedItems, retTree, headerTable, count)

    return retTree, headerTable


def ascendTree(leafNode, prefixPath):
    """ascendTree(如果存在父节点，就记录当前节点的name值)
    Args:
        leafNode   查询的节点对于的nodeTree
        prefixPath 要查询的节点值
    """
    if leafNode.parent is not None:
        prefixPath.append(leafNode.name)
        ascendTree(leafNode.parent, prefixPath)


def findPrefixPath(basePat, treeNode):
    """findPrefixPath 基础数据集
    Args:
        basePat  要查询的节点值
        treeNode 查询的节点所在的当前nodeTree
    Returns:
        condPats 对非basePat的倒叙值作为key,赋值为count数
    """
    condPats = {}
    # 对 treeNode的link进行循环
    while treeNode is not None:
        prefixPath = []
        # 寻找改节点的父节点，相当于找到了该节点的频繁项集
        ascendTree(treeNode, prefixPath)
        # 避免 单独`Z`一个元素，添加了空节点
        if len(prefixPath) > 1:
            # 对非basePat的倒叙值作为key,赋值为count数
            # prefixPath[1:] 变frozenset后，字母就变无序了
            # condPats[frozenset(prefixPath)] = treeNode.count
            condPats[frozenset(prefixPath[1:])] = treeNode.count
        # 递归，寻找改节点的下一个 相同值的链接节点
        treeNode = treeNode.nodeLink
        # print treeNode
    return condPats


def mineTree(inTree, headerTable, minSup, preFix, freqItemList):
    """mineTree(创建条件FP树)
    Args:
        inTree       myFPtree
        headerTable  满足minSup {所有的元素+(value, treeNode)}
        minSup       最小支持项集
        preFix       preFix为newFreqSet上一次的存储记录，一旦没有myHead，就不会更新
        freqItemList 用来存储频繁子项的列表
    """
    # 通过value进行从小到大的排序， 得到频繁项集的key
    # 最小支持项集的key的list集合
    bigL = [v[0] for v in sorted(headerTable.items(), key=lambda p: p[1][0])]
    print('-----', sorted(headerTable.items(), key=lambda p: p[1][0]))
    print('bigL=', bigL)
    # 循环遍历 最频繁项集的key，从小到大的递归寻找对应的频繁项集
    for basePat in bigL:
        # preFix为newFreqSet上一次的存储记录，一旦没有myHead，就不会更新
        newFreqSet = preFix.copy()
        newFreqSet.add(basePat)
        print('newFreqSet=', newFreqSet, preFix)

        freqItemList.append(newFreqSet)
        print('freqItemList=', freqItemList)
        condPattBases = findPrefixPath(basePat, headerTable[basePat][1])
        print('condPattBases=', basePat, condPattBases)

        # 构建FP-tree
        myCondTree, myHead = createTree(condPattBases, minSup)
        print('myHead=', myHead)
        # 挖掘条件 FP-tree, 如果myHead不为空，表示满足minSup {所有的元素+(value, treeNode)}
        if myHead is not None:
            myCondTree.disp(1)
            # 递归 myHead 找出频繁项集
            mineTree(myCondTree, myHead, minSup, newFreqSet, freqItemList)


def loadSimpDat(filename=None, df=None):
    if filename:
        data_lst = np.array(pd.read_excel(filename)).tolist()
        dict_tmp = {}
        for item in data_lst:
            if item[0] not in dict_tmp:
                dict_tmp[item[0]] = [item[1]]
            else:
                dict_tmp[item[0]].append(item[1])
        simpDat = [v for v in dict_tmp.values()]
        return simpDat
    elif df is not None:
        data_lst = np.array(df).tolist()
        dict_tmp = {}
        for item in data_lst:
            if item[0] not in dict_tmp:
                dict_tmp[item[0]] = [item[1]]
            else:
                dict_tmp[item[0]].append(item[1])
        simpDat = [v for v in dict_tmp.values()]
        return simpDat
    else:
        simpDat = [['r', 'z', 'h', 'j', 'p'],
                   ['z', 'y', 'x', 'w', 'v', 'u', 't', 's'],
                   ['z'],
                   ['r', 'x', 'n', 'o', 's'],
                   ['y', 'r', 'x', 'z', 'q', 't', 'p'],
                   ['y', 'z', 'x', 'e', 'q', 's', 't', 'm']]
        return simpDat


def createInitSet(dataSet):
    retDict = {}
    for trans in dataSet:
        if frozenset(trans) not in retDict:
            retDict[frozenset(trans)] = 1
        else:
            retDict[frozenset(trans)] += 1
    return retDict


def evaluation(freqItemList, min_conf, header_table, dataSet):
    """评价函数"""
    freqItemDict = {}
    for values in freqItemList:
        if len(values) >= 2:
            bigcap = 0
            for item in dataSet:
                if set(item) >= values:
                    bigcap += 1
            value_list = []
            for v in values:
                value_list.append(header_table[v][0])
            # 计算置信度
            items_lst = list(values)
            conf = conf_calc(member=bigcap, denominator=header_table[items_lst[0]][0])
            # 计算Kulc
            kulc = kulc_calc(member=bigcap, item_lst=value_list)
            # 计算IR
            ir = IR_calc(cap_num=bigcap, item_lst=value_list)
            print("频繁项集共有{n}对".format(n=len(freqItemList)), "第{n}个".format(n=freqItemList.index(values)),
                  "; 频繁项集=", values, "; 置信度=", round(conf, 4),
                  "; Kulc = ", round(kulc, 4), "; IR不均衡度 = ", round(ir, 4))
            if (conf >= min_conf) & (kulc > min_conf):
                freqItemDict[frozenset(values)] = [conf, kulc, ir]
    return freqItemDict


def conf_calc(member: int, denominator: int):
    """
    计算置信度
    :param member: 分子 各项交集
    :param denominator: 分母 前件在订单中出现的次数
    :return: confidence -> float
    """
    return member/denominator


def kulc_calc(member: int, item_lst):
    """
    计算kulc
    :param member: 分子 各项交集
    :param item_lst: 各项在订单中出现的次数
    :return: kulc -> float
    """
    kulc_nem = 0
    for i in item_lst:
        kulc_nem += conf_calc(member, i)
    return kulc_nem/len(item_lst)


def IR_calc(cap_num: int, item_lst):
    """
    计算IR均衡度
    :param cap_num: 各项交集数量
    :param item_lst: 各项在订单中出现的次数
    :return: ir -> float
    """
    sub_num = item_lst[0]
    add_num = item_lst[0]
    for i in item_lst[1:]:
        sub_num -= i
        add_num += i
    return abs(sub_num)/(add_num-cap_num)


def time_cal(time1, time2):
    """计算时间
    time1: 开始时间
    time2: 结束时间
    """
    times = time2 - time1
    h = int(times//3600)
    m = int((times-h*3600)//60)
    s = int(times-h*3600-m*60)
    return h, m, s


def data_save(data_dict, save_path):
    """
    将数据转化为json保存
    data_dict: 将data转化为json格式数据并保存到文件中
    save_path: 保存的文件路径
    """
    str_json = json.dumps(data_dict)
    with open(save_path,'w') as target:
        # f.write( json.dumps( dict_json,ensure_ascii=False,indent=2 ) )
        target.write(str_json)


if __name__ == '__main__':
    min_supp_num = 5
    min_conf = 0.30

    t0 = time.time()
    # data0 = loadSimpDat()
    data0 = loadSimpDat('association.xlsx')
    # data0 = loadSimpDat(df=run_sql(sql_file.fg_group_sql(begin='20191001', end='20200311')))
    t1 = time.time()
    initSet = createInitSet(data0)
    t2 = time.time()
    # print(initSet)
    myFPtree, myHeaderTab = createTree(initSet, min_supp_num)
    t3 = time.time()
    # # # # myFPtree.disp()
    freqItemList = []
    mineTree(myFPtree, myHeaderTab, min_supp_num, set([]), freqItemList)
    t4 = time.time()
    freqItemDict = evaluation(freqItemList, min_conf=min_conf,
                              header_table=myHeaderTab, dataSet=data0)
    t5 = time.time()
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
    h, m, s = time_cal(t0, t5)
    print("总耗时  {h}h-{m}m-{s}s".format(h=h, m=m, s=s))
