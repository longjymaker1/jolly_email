#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/7/25 10:06
# @Author  : Long
# @Site    : 
# @File    : hanld.py
# @Software: PyCharm

"""
hanld.py 为整个程序入口
"""

from src import sql_file
from src.SQL_base import run_sql, connect_impala
from src.data_conversion import data_to_excel
from src.email_base import emailSend
from src.tools import write_excels_one_sheet, logger_in_file
from src.excel_remove import remove_file
import datetime
import pandas as pd
import numpy as np

today = datetime.datetime.now()
today_day = today.day
today_hour = today.hour
today_minunts = today.minute
today_week = today.weekday() + 1
yesterday = today + datetime.timedelta(days=-1)
this_month_first_day = datetime.datetime(today.year, today.month, 1)
if today.month == 1:
    # last_month_first_day = datetime.datetime(today.year, today.month - 1, 1)
    last_month_first_day = datetime.datetime(year=today.year - 1, month=12, day=1)
    last_month_first_day_yoy = datetime.datetime(year=today.year - 2, month=12, day=1)
else:
    last_month_first_day = datetime.datetime(today.year, today.month - 1, 1)
    last_month_first_day_yoy = datetime.datetime(today.year - 1, today.month - 1, 1)

this_month_first_day_yoy = datetime.datetime(today.year - 1, today.month, 1)
yesterday_yoy = datetime.datetime(yesterday.year - 1, yesterday.month, yesterday.day)
this_year_first_day = datetime.datetime(year=today.year, month=1, day=1)

last_month_first_str = datetime.datetime.strftime(last_month_first_day, "%Y%m%d")
yesterday_str = datetime.datetime.strftime(yesterday, "%Y%m%d")
last_month_first_day_yoy_str = datetime.datetime.strftime(last_month_first_day_yoy, "%Y%m%d")
yesterday_yoy_str = datetime.datetime.strftime(yesterday_yoy, "%Y%m%d")
base_file_path = r"F:\pro\JollychicEmail\excel_file"


def totle_sum(df):
    """
    跳过非int, float类型数据计算合计
    """
    num = 0
    for i in df:
        if type(i) == int or type(i) == float:
            num += i
    return num


def column_format(df, col, dec):
    """
    数据格式化
    """
    return np.around(df[col], decimals=dec)


def column_percent_format(df, col, dec):
    """
    百分比格式化
    """
    return np.around(df[col] * 100, decimals=dec).astype(str) + "%"


@logger_in_file('reportlog.log')
def KA_data(user_lst: list):
    """母婴童KA商家数据发送"""
    data = run_sql(sql_file.KA_saler)
    datas = [{'母婴童KA数据': [(data, 'data'), ]}]
    file_path = data_to_excel(filename='母婴童KA数据', data_list=datas)
    my_sender = emailSend(users=user_lst, title='母婴童KA商家数据', file_path=file_path)
    my_sender.sender()


@logger_in_file('reportlog.log')
def Price_brand(user_lst: list):
    """母婴童价格带数据发送"""
    data = run_sql(sql_file.price_brand())
    datas = [{'价格带商品数量': [(data, 'data'), ]}]
    file_path = data_to_excel(filename='价格带商品数量', data_list=datas)
    my_sender = emailSend(users=user_lst, title='价格带商品数量', file_path=file_path)
    my_sender.sender()


@logger_in_file('reportlog.log')
def On_Sale(user_lst: list):
    """on_sale数据发送"""
    months = today.month - 1
    file_name = str(months) + '月on_sale'

    data0 = run_sql(sql_file.on_sale_sql)
    datas = [{'no_slae': [(data0, 'data')]}]
    file_path = data_to_excel(filename=file_name, data_list=datas)
    my_sender = emailSend(users=user_lst, title=file_name, file_path=file_path)
    my_sender.sender()


@logger_in_file('reportlog.log')
def Goods_Sale_Level():
    """获取商品分层基础数据"""
    file_name = '商品分层data'
    data0 = run_sql(sql_file.goods_sale_level)
    datas = [{'sale_level': [(data0, 'data')]}]
    data_to_excel(filename=file_name, data_list=datas)


@logger_in_file('reportlog.log')
def dau_data():
    """导出dau数据"""
    file_name = 'dau_data'
    data0 = run_sql(sql_file.dau_sql)
    datas = [{'dau_data_01': [(data0, 'data')]}]
    data_to_excel(filename=file_name, data_list=datas)


@logger_in_file('reportlog.log')
def margin_warning(user_lst):
    """毛利预警"""
    file_name = "毛利预警"
    data0 = run_sql(sql_file.margin_warning)
    datas = [{'margin_warning': [(data0, 'data')]}]
    file_path = data_to_excel(filename=file_name, data_list=datas)
    my_sender = emailSend(users=user_lst, title=file_name, file_path=file_path)
    my_sender.sender()


@logger_in_file('reportlog.log')
def sku_msg():
    """获取商品分层基础数据"""
    file_name = '童装sku'
    data0 = run_sql(sql_file.sku_sql)
    datas = [{'童装sku': [(data0, 'data')]}]
    data_to_excel(filename=file_name, data_list=datas)


@logger_in_file('reportlog.log')
def stock_msg():
    """获取商品分层基础数据"""
    file_name = '订单sku'
    data0 = run_sql(sql_file.stock_msg)
    datas = [{'订单sku': [(data0, 'data')]}]
    data_to_excel(filename=file_name, data_list=datas)


@logger_in_file('reportlog.log')
def zhouQianNan(user_lst):
    file_name = '日韩供应商'
    data0 = run_sql(sql_file.zhouquannan)
    datas = [{'supplier': [(data0, 'data')]}]
    file_path = data_to_excel(filename=file_name, data_list=datas)
    my_sender = emailSend(users=user_lst, title=file_name, file_path=file_path)
    my_sender.sender()


@logger_in_file('reportlog.log')
def act_hour_goods():
    data0 = run_sql(sql_file.act_gmv_hour_1)
    data0.sort_values(by=['goods_id', 'time_hour'])
    data_msg = data0[['category_group', 'cate_level1_name', 'data_date', 'goods_id', 'goods_name']]
    date_datas = data0[['goods_id', 'time_hour', 'revenue']]
    date_hour_datas = data0[['goods_id', 'order_num', 'num', 'revenue']]
    data_msg = data_msg.drop_duplicates()
    new_data = pd.pivot_table(date_datas, index='goods_id', columns='time_hour', values='revenue')
    new_data = new_data.reset_index()
    new_data2 = date_hour_datas.groupby('goods_id').sum().reset_index()
    new_data2['unit_price'] = new_data2['revenue'] / new_data2['num']
    new_data2['order_unit_num'] = new_data2['num'] / new_data2['order_num']
    new_data2['order_unit_price'] = new_data2['revenue'] / new_data2['order_num']
    new_data2 = new_data2[['goods_id', 'unit_price', 'order_unit_num', 'order_unit_price']]
    send_pd = pd.merge(left=data_msg, right=new_data2, left_on='goods_id', right_on='goods_id', how='left')
    send_pd = pd.merge(left=send_pd, right=new_data, left_on='goods_id', right_on='goods_id', how='left')
    return send_pd


@logger_in_file('reportlog.log')
def goods_hour_dau():
    data0 = run_sql(sql_file.goods_hour_dau)

    def new_columns_lst(old_columns: list):
        lst_columns = []
        for i in old_columns:
            if type(i[1]) == int:
                lst_columns.append(i[1])
            else:
                lst_columns.append(i[0])
        return lst_columns

    new_data = pd.pivot_table(data=data0, index=['category_group', 'cate_level1_name', 'goods_id'],
                              columns=['时'],
                              values=['点击uv']).reset_index()
    old_columns = list(new_data.columns)
    new_columns_lst(old_columns)
    new_data.columns = new_columns_lst(old_columns)
    new_data['类型'] = '点击uv'

    new_data2 = pd.pivot_table(data=data0, index=['category_group', 'cate_level1_name', 'goods_id'],
                               columns=['时'],
                               values=['点击pv']).reset_index()
    old_columns = list(new_data2.columns)
    new_columns_lst(old_columns)
    new_data2.columns = new_columns_lst(old_columns)
    new_data2['类型'] = '点击pv'

    new_data3 = pd.pivot_table(data=data0, index=['category_group', 'cate_level1_name', 'goods_id'],
                               columns=['时'],
                               values=['加购uv']).reset_index()
    old_columns = list(new_data3.columns)
    new_columns_lst(old_columns)
    new_data3.columns = new_columns_lst(old_columns)
    new_data3['类型'] = '加购uv'

    new_data4 = pd.pivot_table(data=data0, index=['category_group', 'cate_level1_name', 'goods_id'],
                               columns=['时'],
                               values=['加购pv']).reset_index()
    old_columns = list(new_data4.columns)
    new_columns_lst(old_columns)
    new_data4.columns = new_columns_lst(old_columns)
    new_data4['类型'] = '加购pv'

    return_df = pd.concat([new_data, new_data2, new_data3, new_data4])
    new_lst = ['category_group', 'cate_level1_name', 'goods_id', '类型']

    for i in list(return_df.columns):
        if type(i) == int:
            new_lst.append(i)
    return_df = return_df[new_lst].sort_index(by=['category_group', 'cate_level1_name', 'goods_id', '类型'],
                                              ascending=[True, True, True, True])
    return return_df


@logger_in_file('reportlog.log')
def category_hour_dau():
    data0 = run_sql(sql_file.hour_dau)

    def new_columns_lst(old_columns: list):
        lst_columns = []
        for i in old_columns:
            if type(i[1]) == int:
                lst_columns.append(i[1])
            else:
                lst_columns.append(i[0])
        return lst_columns

    new_data = pd.pivot_table(data=data0, index=['category_group', 'cate_level1_name'],
                              columns=['时'],
                              values=['点击uv']).reset_index()
    old_columns = list(new_data.columns)
    new_data.columns = new_columns_lst(old_columns)
    new_data['类型'] = '点击uv'

    new_data2 = pd.pivot_table(data=data0, index=['category_group', 'cate_level1_name'],
                               columns=['时'],
                               values=['点击pv']).reset_index()
    old_columns = list(new_data2.columns)
    new_data2.columns = new_columns_lst(old_columns)
    new_data2['类型'] = '点击pv'

    new_data3 = pd.pivot_table(data=data0, index=['category_group', 'cate_level1_name'],
                               columns=['时'],
                               values=['加购uv']).reset_index()
    old_columns = list(new_data3.columns)
    new_data3.columns = new_columns_lst(old_columns)
    new_data3['类型'] = '加购uv'

    new_data4 = pd.pivot_table(data=data0, index=['category_group', 'cate_level1_name'],
                               columns=['时'],
                               values=['加购pv']).reset_index()
    old_columns = list(new_data4.columns)
    new_data4.columns = new_columns_lst(old_columns)
    new_data4['类型'] = '加购pv'

    return_df = pd.concat([new_data, new_data2, new_data3, new_data4])

    new_lst = ['category_group', 'cate_level1_name', '类型']

    for i in list(return_df.columns):
        if type(i) == int:
            new_lst.append(i)

    return_df = return_df[new_lst].sort_index(by=['category_group', 'cate_level1_name', '类型'],
                                              ascending=[True, True, False])
    return return_df


@logger_in_file('reportlog.log')
def act_hour(user_lst):
    file_name = '黑五小时数据'
    data0 = run_sql(sql_file.act_gmv_hour_0)
    cates_set = set(data0['category_group'])
    data_lst = []
    for item in cates_set:
        data1 = data0[data0['category_group'] == item].sort_values(by='time_hour')
        new_data = pd.pivot_table(data1, index=['category_group', 'cate_level1_name', 'days0'],
                                  columns=['time_hour'],
                                  values=['revenue'])
        new_data = new_data.reset_index().sort_values(by=['cate_level1_name', 'days0'], ascending=[True, False])
        columns = []
        for i in new_data.columns:
            if i[0] != 'revenue':
                columns.append(i[0])
            else:
                columns.append(i[1])
        new_data.columns = columns
        data_lst.append((new_data, item))

    datas = [{'GMV': data_lst}]
    try:
        goods_gmv = act_hour_goods()
        datas.append({'goods_gmv': [goods_gmv]})
    except Exception:
        print("goods_gmv失败")
    try:
        goods_dau_df = goods_hour_dau()
        datas.append({'goods_dau': [goods_dau_df]})
    except Exception:
        print("goods_dau失败")
    try:
        category_hour_df = category_hour_dau()
        datas.append({'category_dau': [category_hour_df]})
    except Exception:
        print("category_dau失败")
    file_path = data_to_excel(filename=file_name, data_list=datas)
    my_sender = emailSend(users=user_lst, title=file_name, file_path=file_path)
    my_sender.sender()


@logger_in_file('reportlog.log')
def act_hour_matebase():
    drop_sql, create_sql = sql_file.act_hour()
    try:
        print("删除表")
        run_sql(drop_sql, is_return=False)
        print("删除完成")
    except Exception as e:
        print("删除错误，创建表")
        if "Table does not exist" in str(e):
            run_sql(create_sql, is_return=False)
            return 0

    try:
        print("创建表")
        run_sql(create_sql, is_return=False)
        print("创建完成")
    except Exception as e:
        print("创建失败")
        if "Table already exists" in str(e):
            print("创建失败重新删除表")
            run_sql(drop_sql, is_return=False)
            print("删除表完成")
            run_sql(create_sql, is_return=False)
            print("创建表")
            return 0


@logger_in_file('reportlog.log')
def bu_daily_report():
    drop_sql, create_sql = sql_file.bu_daily_report_sql()
    try:
        print("删除表")
        run_sql(drop_sql, is_return=False)
        print("删除完成")
    except Exception as e:
        print("删除错误，创建表")
        if "Table does not exist" in str(e):
            run_sql(create_sql, is_return=False)
            return 0

    try:
        print("创建表")
        run_sql(create_sql, is_return=False)
        print("创建完成")
    except Exception as e:
        print("创建失败")
        if "Table already exists" in str(e):
            print("创建失败重新删除表")
            run_sql(drop_sql, is_return=False)
            print("删除表完成")
            run_sql(create_sql, is_return=False)
            print("创建表")
            return 0


@logger_in_file('reportlog.log')
def kids_data_goods_report(period: str, user_lst):
    """
    童装商品月&周报
    :param period: week_month
    :param user_lst: 接受邮件的用户列表
    :return:
    """
    file_name = '婴童时尚_单品销售明细（带属性）_{be}'.format(be=period)
    data0 = run_sql(sql_file.kids_week_month_sql(period))
    datas = [{'data': [(data0, 'data')]}]
    file_path = data_to_excel(filename=file_name, data_list=datas)
    my_sender = emailSend(users=user_lst, title=file_name, file_path=file_path)
    my_sender.sender()


@logger_in_file('reportlog.log')
def kids_data_cate_report(period: str, user_lst):
    """
    童装三级类目月&周报
    :param period: week_month
    :param user_lst: 接受邮件的用户列表
    :return:
    """
    file_name = '婴童时尚_分四级类目_{be}'.format(be=period)
    data0 = run_sql(sql_file.kids_week_month_cate_sql(period))
    datas = [{'data': [(data0, 'data')]}]
    file_path = data_to_excel(filename=file_name, data_list=datas)
    my_sender = emailSend(users=user_lst, title=file_name, file_path=file_path)
    my_sender.sender()


@logger_in_file('reportlog.log')
def supp_week_report(begin, end, user_lst):
    """
    供应商周报
    :param begin: 开始时间
    :param end: 截止时间
    :param user_lst: 接受邮件用户列表
    :return: None
    """
    datadf = run_sql(sql_file.supp_week_sql(begin, end))

    new_df0 = datadf.pivot_table(
        index=['supp_id', 'code', 'supp_name', 'status', 'provider_type', 'cate_level1_name', 'great_time'],
        values=['goods_num', 'sku_num', 'new_goods_num', 'sale_goods_num', 'num', 'revenue', 'cost_with_vat'],
        aggfunc=sum).reset_index()
    new_df1 = datadf.pivot_table(index=['code'],
                                 values=['cost_rmb', 'goods_num', 'sku_num', 'new_goods_num', 'sale_goods_num'],
                                 aggfunc=np.mean).reset_index()
    new_df2 = datadf.pivot_table(index=['code'],
                                 values=['supp_name'],
                                 aggfunc=len).reset_index()
    re_df = pd.merge(left=new_df0, right=new_df1, left_on='code', right_on='code', how='left')
    re_df = pd.merge(left=re_df, right=new_df2, left_on='code', right_on='code', how='left')
    re_df['margin'] = 1 - re_df['cost_with_vat'] / re_df['revenue']  # 计算毛利
    re_df['goods_sale_rate'] = re_df['sale_goods_num_x'] / re_df['goods_num_x']  # 计算动销率
    re_df['gmv_mean_month'] = re_df['revenue'] / re_df['supp_name_y']  # 计算月均GMV
    re_df['unit_base_goods'] = re_df['num'] / re_df['goods_num_x']  # 计算款均销量
    re_df['unit_price_num'] = re_df['revenue'] / re_df['num']  # 计算件单价

    re_df['goods_num_y'] = re_df.apply(column_format, axis=1, col='goods_num_y', dec=1)
    re_df['sku_num_y'] = re_df.apply(column_format, axis=1, col='sku_num_y', dec=1)
    re_df['new_goods_num_y'] = re_df.apply(column_format, axis=1, col='new_goods_num_y', dec=1)
    re_df['sale_goods_num_y'] = re_df.apply(column_format, axis=1, col='sale_goods_num_y', dec=1)
    re_df['goods_sale_rate'] = re_df.apply(column_percent_format, axis=1, col='goods_sale_rate', dec=1)
    re_df['gmv_mean_month'] = re_df.apply(column_format, axis=1, col='gmv_mean_month', dec=1)
    re_df['unit_base_goods'] = re_df.apply(column_format, axis=1, col='unit_base_goods', dec=1)
    re_df['unit_price_num'] = re_df.apply(column_format, axis=1, col='unit_price_num', dec=1)
    re_df['cost_rmb'] = re_df.apply(column_format, axis=1, col='cost_rmb', dec=1)
    re_df['margin'] = re_df.apply(column_percent_format, axis=1, col='margin', dec=1)

    re_df = re_df[
        ['supp_id', 'code', 'supp_name_x', 'status', 'provider_type', 'cate_level1_name', 'supp_name_y', 'great_time',
         'goods_num_y', 'sku_num_y', 'new_goods_num_y', 'sale_goods_num_y', 'goods_sale_rate', 'num', 'revenue',
         'gmv_mean_month', 'unit_base_goods', 'unit_price_num', 'margin', 'cost_rmb']]
    re_df.columns = ['供应商id', '供应商编码', '供应商名称', '合作状态', '供应商类型', '主营类目', '本年经营月数', '合作时间',
                     '月均在架商品数', '月均sku数量', '月均新品数量', '月均有销售商品数', '动销率', '销售数量', 'GMV',
                     '月均GMV', '款均销量', '件单价', '毛利率', '平均成本(￥)']

    file_path = write_excels_one_sheet('供应商周报', [1, 1],
                                       供应商=re_df)
    my_sender = emailSend(users=user_lst, title='供应商周报', file_path=file_path)
    my_sender.sender()


def volume_weight(user_lst):
    file_name = '抛重_sku'
    data0 = run_sql(sql_file.volume_weight())
    datas = [{'data': [(data0, 'data')]}]
    file_path = data_to_excel(filename=file_name, data_list=datas)
    my_sender = emailSend(users=user_lst, title=file_name, file_path=file_path)
    my_sender.sender()


def kids_value(user_lst):
    """通过尺码判断标签"""
    file_name = '童装标签'
    data0 = run_sql(sql_file.kids_value)
    datas = [{'data': [(data0, 'data')]}]
    file_path = data_to_excel(filename=file_name, data_list=datas)
    my_sender = emailSend(users=user_lst, title=file_name, file_path=file_path)
    my_sender.sender()


def stock_data():
    file_name = '库存数据'
    data0 = run_sql(sql_file.stock_data_msg)
    datas = [{'data': [(data0, 'data')]}]
    data_to_excel(filename=file_name, data_list=datas)
    # my_sender = emailSend(users=user_lst, title=file_name, file_path=file_path)
    # my_sender.sender()


@logger_in_file('reportlog.log')
def sku_sale_tmp(user_lst):
    file_name = 'sku_sale'
    data0 = run_sql(sql_file.sku_sale_tmp_msg)
    datas = [{'data': [(data0, 'data')]}]
    file_path = data_to_excel(filename=file_name, data_list=datas)
    my_sender = emailSend(users=user_lst, title=file_name, file_path=file_path)
    my_sender.sender()


def stock_goods_sale():
    file_name = '本地仓_缺货_未拆单'
    data0 = run_sql(sql_file.stock_goods_sale)
    datas = [{'data': [(data0, 'data')]}]
    data_to_excel(filename=file_name, data_list=datas)
    # my_sender = emailSend(users=user_lst, title=file_name, file_path=file_path)
    # my_sender.sender()


def sql_tmp(user_lst):
    file_name = '预算参考数据'
    data0 = run_sql(sql_file.tmp_sql())
    datas = [{'data': [(data0, 'data')]}]
    data_to_excel(filename=file_name, data_list=datas)
    # my_sender = emailSend(users=user_lst, title=file_name, file_path=file_path)
    # my_sender.sender()


@logger_in_file('reportlog.log')
def rpts_daily_insert():
    begin_data_str = '2018-02-10'
    begin_data = datetime.datetime.strptime(begin_data_str, '%Y-%m-%d')
    begin_data + datetime.timedelta(days=14)

    last_data = datetime.datetime.strptime('2019-04-01', "%Y-%m-%d")
    # print(end_data<tmp_data)

    flag = True
    bgd = begin_data
    while flag:
        edd = bgd + datetime.timedelta(days=14)
        print("运行sql")
        print(bgd, edd)
        bgd_str = datetime.datetime.strftime(bgd, "%Y%m%d")
        edd_str = datetime.datetime.strftime(edd, "%Y%m%d")
        print("bgd转化为字符->", bgd_str)
        print("edd转化为字符->", edd_str)
        insert_sql = sql_file.rpts_daily_2018(bgd_str, edd_str)
        print(insert_sql)
        run_sql(insert_sql, is_return=False)
        print("运行结束")
        if edd >= last_data:
            flag = False
        else:
            bgd = edd


# @logger_in_file('reportlog.log')
def bu_report_daily_email(begin: str, end: str, begin_last_year: str, end_last_year: str, user_lst):
    """
    BU日报，gmv，毛利，dau
    :param begin: 开始时间
    :param end: 截止时间
    :param begin_last_year: 同期开始时间
    :param end_last_year: 同期截止时间
    :return: pass
    """
    msg = sql_file.bu_report_daily(begin, end, begin_last_year, end_last_year)
    datadf = run_sql(msg)
    datadf['data_date'] = pd.to_datetime(datadf['data_date'], format="%Y-%m-%d")

    def this_year_gmv_self():
        """
        计算今年自营GMV
        :return: df
        """
        last_month_data = datadf[(datadf['is_pop'] == 0) & (datadf['data_date'] >= last_month_first_day) & (
                datadf['data_date'] < this_month_first_day)]
        last_month_all = pd.DataFrame(last_month_data.groupby('bu')['gmv'].sum())
        last_month_all.columns = [str(today.month - 1) + '月合计']
        last_month_all = last_month_all.T
        last_month_all['data_date'] = '上月合计'
        last_month_all = last_month_all[['data_date', '3C', '家居', '美妆', '母婴童', '时尚', '手机']]
        this_month_gmv = datadf[(datadf['is_pop'] == 0) & (datadf['data_date'] >= this_month_first_day) & (
                datadf['data_date'] <= yesterday)]
        this_month_gmv_df = this_month_gmv.pivot_table(columns='bu', index='data_date', values='gmv',
                                                       aggfunc='sum').reset_index()
        this_month_gmv_df.loc['合计'] = this_month_gmv_df.apply(totle_sum, axis=0)
        this_month_gmv_df = this_month_gmv_df[['data_date', '3C', '家居', '美妆', '母婴童', '时尚', '手机']]
        this_year_gmv_return = pd.concat([last_month_all, this_month_gmv_df], axis=0, sort=True)[
            ['data_date', '3C', '家居', '美妆', '母婴童', '时尚', '手机']]
        this_year_gmv_return['合计'] = this_year_gmv_return.apply(totle_sum, axis=1)
        for i in this_year_gmv_return.columns:
            if i == 'data_date':
                pass
            else:
                this_year_gmv_return[i] = this_year_gmv_return.apply(column_format, axis=1, col=i, dec=0)
        return this_year_gmv_return

    def this_year_gmv_pop():
        """
        计算今年自营+POP  GMV
        """
        last_month_data = datadf[(datadf['data_date'] >= last_month_first_day) &
                                 (datadf['data_date'] < this_month_first_day)]
        last_month_all = pd.DataFrame(last_month_data.groupby('bu')['gmv'].sum())
        last_month_all.columns = [str(today.month - 1) + '月合计']
        last_month_all = last_month_all.T
        last_month_all['data_date'] = '上月合计'
        last_month_all = last_month_all[['data_date', '3C', '家居', '美妆', '母婴童', '时尚', '手机']]
        this_month_gmv = datadf[(datadf['data_date'] >= this_month_first_day) &
                                (datadf['data_date'] <= yesterday)]
        this_month_gmv_df = this_month_gmv.pivot_table(columns='bu', index='data_date', values='gmv',
                                                       aggfunc='sum').reset_index()
        this_month_gmv_df.loc['合计'] = this_month_gmv_df.apply(totle_sum, axis=0)
        this_month_gmv_df = this_month_gmv_df[['data_date', '3C', '家居', '美妆', '母婴童', '时尚', '手机']]
        this_year_gmv_return = pd.concat([last_month_all, this_month_gmv_df], axis=0, sort=True)[
            ['data_date', '3C', '家居', '美妆', '母婴童', '时尚', '手机']]
        this_year_gmv_return['合计'] = this_year_gmv_return.apply(totle_sum, axis=1)
        for i in this_year_gmv_return.columns:
            if i == 'data_date':
                pass
            else:
                this_year_gmv_return[i] = this_year_gmv_return.apply(column_format, axis=1, col=i, dec=0)
        return this_year_gmv_return

    def last_year_gmv_self():
        """
        计算去年自营GMV
        """
        last_month_data = datadf[(datadf['is_pop'] == 0) & (datadf['data_date'] >= last_month_first_day_yoy) & (
                datadf['data_date'] < this_month_first_day_yoy)]
        last_month_all = pd.DataFrame(last_month_data.groupby('bu')['gmv'].sum())
        last_month_all.columns = [str(today.month - 1) + '月合计']
        last_month_all = last_month_all.T
        last_month_all['data_date'] = '上月合计'
        last_month_all = last_month_all[['data_date', '3C', '家居', '美妆', '母婴童', '时尚', '手机']]
        this_month_gmv = datadf[(datadf['is_pop'] == 0) & (datadf['data_date'] >= this_month_first_day_yoy) & (
                datadf['data_date'] <= yesterday_yoy)]
        this_month_gmv_df = this_month_gmv.pivot_table(columns='bu', index='data_date', values='gmv',
                                                       aggfunc='sum').reset_index()
        this_month_gmv_df.loc['合计'] = this_month_gmv_df.apply(totle_sum, axis=0)
        this_month_gmv_df = this_month_gmv_df[['data_date', '3C', '家居', '美妆', '母婴童', '时尚', '手机']]
        last_year_gmv_return = pd.concat([last_month_all, this_month_gmv_df], axis=0, sort=True)[
            ['data_date', '3C', '家居', '美妆', '母婴童', '时尚', '手机']]
        last_year_gmv_return['合计'] = last_year_gmv_return.apply(totle_sum, axis=1)
        for i in last_year_gmv_return.columns:
            if i == 'data_date':
                pass
            else:
                last_year_gmv_return[i] = last_year_gmv_return.apply(column_format, axis=1, col=i, dec=0)
        return last_year_gmv_return

    def last_year_gmv_pop():
        """
        计算去年自营+POP  GMV
        """
        last_month_data = datadf[(datadf['data_date'] >= last_month_first_day_yoy) &
                                 (datadf['data_date'] < this_month_first_day_yoy)]
        last_month_all = pd.DataFrame(last_month_data.groupby('bu')['gmv'].sum())
        last_month_all.columns = [str(today.month - 1) + '月合计']
        last_month_all = last_month_all.T
        last_month_all['data_date'] = '上月合计'
        last_month_all = last_month_all[['data_date', '3C', '家居', '美妆', '母婴童', '时尚', '手机']]
        this_month_gmv = datadf[(datadf['data_date'] >= this_month_first_day_yoy) &
                                (datadf['data_date'] <= yesterday_yoy)]
        this_month_gmv_df = this_month_gmv.pivot_table(columns='bu', index='data_date', values='gmv',
                                                       aggfunc='sum').reset_index()
        this_month_gmv_df.loc['合计'] = this_month_gmv_df.apply(totle_sum, axis=0)
        this_month_gmv_df = this_month_gmv_df[['data_date', '3C', '家居', '美妆', '母婴童', '时尚', '手机']]
        last_year_gmv_return = pd.concat([last_month_all, this_month_gmv_df], axis=0, sort=True)[
            ['data_date', '3C', '家居', '美妆', '母婴童', '时尚', '手机']]
        last_year_gmv_return['合计'] = last_year_gmv_return.apply(totle_sum, axis=1)
        for i in last_year_gmv_return.columns:
            if i == 'data_date':
                pass
            else:
                last_year_gmv_return[i] = last_year_gmv_return.apply(column_format, axis=1, col=i, dec=0)
        return last_year_gmv_return

    def this_year_dau():
        """
        计算当月每日dau
        """
        this_month = datadf[(datadf['data_date'] >= this_month_first_day) & (datadf['data_date'] <= yesterday)]
        this_month_dau = this_month.pivot_table(columns='bu', index='data_date', values='点击uv',
                                                aggfunc='sum').reset_index()
        this_month_dau = this_month_dau[['data_date', '3C', '家居', '美妆', '母婴童', '时尚', '手机']]
        this_month_dau['合计'] = this_month_dau.apply(totle_sum, axis=1)
        return this_month_dau

    def this_year_margin():
        """
        计算当月毛利率
        """
        last_month = datadf[(datadf['is_pop'] == 0) & (datadf['data_date'] >= last_month_first_day) & (
                datadf['data_date'] < this_month_first_day)]
        last_month_gmv_totle = pd.DataFrame(last_month.groupby('bu')['gmv'].sum())
        last_month_cost_totle = pd.DataFrame(last_month.groupby('bu')['成本'].sum())
        last_month_margin = pd.concat([last_month_gmv_totle, last_month_cost_totle], axis=1)
        last_month_margin.loc['合计'] = last_month_margin.apply(totle_sum, axis=0)
        last_month_margin['毛利率'] = 1 - last_month_margin['成本'] / last_month_margin['gmv']
        last_month_margin.loc['合计', '毛利率'] = 1 - last_month_margin.loc['合计', '成本'] / \
                                             last_month_margin.loc['合计', 'gmv']
        last_month_margin_df = pd.DataFrame(last_month_margin['毛利率'])
        last_month_margin_df = last_month_margin_df.T
        last_month_margin_df['data_date'] = '上月合计'
        last_month_margin_df = last_month_margin_df[['data_date', '3C', '家居', '美妆', '母婴童', '时尚', '手机', '合计']]

        this_month_df = datadf[(datadf['is_pop'] == 0) & (datadf['data_date'] >= this_month_first_day) & (
                datadf['data_date'] <= yesterday)]
        this_month_margin = this_month_df.pivot_table(columns='bu', index='data_date', values=['gmv', '成本'],
                                                      aggfunc='sum').reset_index()
        this_month_margin[('gmv', '合计')] = this_month_margin[
            [('gmv', '3C'), ('gmv', '家居'), ('gmv', '手机'), ('gmv', '时尚'),
             ('gmv', '母婴童'), ('gmv', '美妆')]].apply(sum, axis=1)
        this_month_margin[('成本', '合计')] = this_month_margin[
            [('成本', '3C'), ('成本', '家居'), ('成本', '手机'),
             ('成本', '时尚'), ('成本', '母婴童'), ('成本', '美妆')]].apply(sum, axis=1)
        this_month_margin.loc['合计'] = this_month_margin.apply(totle_sum, axis=0)
        colums_lst = [i[1] for i in this_month_margin.columns]
        colums_lst.remove('')
        colums_lst = list(set(colums_lst))
        for i in colums_lst:
            this_month_margin[('毛利率', i)] = 1 - this_month_margin[('成本', i)] / this_month_margin[('gmv', i)]
        new_columns_tmp = [('data_date', '')]
        new_columns_lst = ['data_date']
        for i in this_month_margin.columns:
            if i[0] == '毛利率':
                new_columns_tmp.append(i)
                new_columns_lst.append(i[1])
        this_month_margin_df = this_month_margin[new_columns_tmp]
        this_month_margin_df.columns = new_columns_lst
        this_month_margin_df = this_month_margin_df[['data_date', '3C', '家居', '美妆', '母婴童', '时尚', '手机', '合计']]
        this_year_margin_df = pd.concat([last_month_margin_df, this_month_margin_df], axis=0)
        this_year_margin_df.loc['合计', 'data_date'] = '本月合计'
        this_year_margin_df.index = [i for i in range(len(this_year_margin_df))]
        for i in this_year_margin_df.columns:
            if i == 'data_date':
                pass
            else:
                this_year_margin_df[i] = this_year_margin_df.apply(column_percent_format, axis=1, col=i, dec=1)
        return this_year_margin_df

    def last_year_margin():
        """
        计算去年同期毛利
        """
        last_month = datadf[(datadf['is_pop'] == 0) & (datadf['data_date'] >= last_month_first_day_yoy) & (
                datadf['data_date'] < this_month_first_day_yoy)]
        last_month_gmv_totle = pd.DataFrame(last_month.groupby('bu')['gmv'].sum())
        last_month_cost_totle = pd.DataFrame(last_month.groupby('bu')['成本'].sum())
        last_month_margin = pd.concat([last_month_gmv_totle, last_month_cost_totle], axis=1)
        last_month_margin.loc['合计'] = last_month_margin.apply(totle_sum, axis=0)
        last_month_margin['毛利率'] = 1 - last_month_margin['成本'] / last_month_margin['gmv']
        last_month_margin.loc['合计', '毛利率'] = 1 - last_month_margin.loc['合计', '成本'] / \
                                             last_month_margin.loc['合计', 'gmv']
        last_month_margin_df = pd.DataFrame(last_month_margin['毛利率'])
        last_month_margin_df = last_month_margin_df.T
        last_month_margin_df['data_date'] = '上月合计'
        last_month_margin_df = last_month_margin_df[['data_date', '3C', '家居', '美妆', '母婴童', '时尚', '手机', '合计']]

        this_month_df = datadf[(datadf['is_pop'] == 0) & (datadf['data_date'] >= this_month_first_day_yoy) & (
                datadf['data_date'] <= yesterday_yoy)]
        this_month_margin = this_month_df.pivot_table(columns='bu', index='data_date', values=['gmv', '成本'],
                                                      aggfunc='sum').reset_index()
        this_month_margin[('gmv', '合计')] = this_month_margin[
            [('gmv', '3C'), ('gmv', '家居'), ('gmv', '手机'), ('gmv', '时尚'),
             ('gmv', '母婴童'), ('gmv', '美妆')]].apply(sum, axis=1)
        this_month_margin[('成本', '合计')] = this_month_margin[
            [('成本', '3C'), ('成本', '家居'), ('成本', '手机'),
             ('成本', '时尚'), ('成本', '母婴童'), ('成本', '美妆')]].apply(sum, axis=1)
        this_month_margin.loc['合计'] = this_month_margin.apply(totle_sum, axis=0)
        colums_lst = [i[1] for i in this_month_margin.columns]
        colums_lst.remove('')
        colums_lst = list(set(colums_lst))
        for i in colums_lst:
            this_month_margin[('毛利率', i)] = 1 - this_month_margin[('成本', i)] / this_month_margin[('gmv', i)]
        new_columns_tmp = [('data_date', '')]
        new_columns_lst = ['data_date']
        for i in this_month_margin.columns:
            if i[0] == '毛利率':
                new_columns_tmp.append(i)
                new_columns_lst.append(i[1])
        this_month_margin_df = this_month_margin[new_columns_tmp]
        this_month_margin_df.columns = new_columns_lst
        this_month_margin_df = this_month_margin_df[['data_date', '3C', '家居', '美妆', '母婴童', '时尚', '手机', '合计']]
        this_year_margin_df = pd.concat([last_month_margin_df, this_month_margin_df], axis=0)
        this_year_margin_df.loc['合计', 'data_date'] = '本月合计'
        this_year_margin_df.index = [i for i in range(len(this_year_margin_df))]
        for i in this_year_margin_df.columns:
            if i == 'data_date':
                pass
            else:
                this_year_margin_df[i] = this_year_margin_df.apply(column_percent_format, axis=1, col=i, dec=1)
        return this_year_margin_df

    this_year_gmv_self_df = this_year_gmv_self()
    this_year_gmv_all_df = this_year_gmv_pop()
    this_year_margin_df = this_year_margin()
    this_year_dau_df = this_year_dau()

    last_year_gmv_self_df = last_year_gmv_self()
    last_year_gmv_all_df = last_year_gmv_pop()
    last_year_margin_df = last_year_margin()
    # print(last_year_margin_df)
    file_path = write_excels_one_sheet('BU日报', [4, 2],
                                       GMV_2019=this_year_gmv_self_df,
                                       GMV_POP_2019=this_year_gmv_all_df,
                                       margin_2019=this_year_margin_df,
                                       DAU_2019=this_year_dau_df,
                                       GMV_2018=last_year_gmv_self_df,
                                       GMV_POP_2018=last_year_gmv_all_df,
                                       margin_2018=last_year_margin_df)
    my_sender = emailSend(users=user_lst, title='BU日报', file_path=file_path)
    my_sender.sender()


@logger_in_file('reportlog.log')
def user_resale_week_report(user_lst, cate_level=2):
    """
    用户复购、流失周报
    :param cate_level: 核算等级, 默认到二级类目
    :param user_lst: 邮件列表
    :return: None
    """
    if cate_level == 1:
        msg = sql_file.user_resale_first_week()
        file_name = '新用户留存率一级类目'
    else:
        msg = sql_file.user_resale_second_week()
        file_name = '新用户留存率二级类目'
    datadf = run_sql(msg)
    # datadf[i] = datadf.apply(column_format, axis=1, col=i, dec=0)
    # datadf[i] = datadf.apply(column_percent_format, axis=1, col=i, dec=1)
    for i in datadf.columns:
        if 'gmv' in i:
            datadf[i] = datadf.apply(column_format, axis=1, col=i, dec=0)
        elif '客单价' in i:
            datadf[i] = datadf.apply(column_format, axis=1, col=i, dec=2)
        elif i in ['流失率_2次', '流失率_3次', '流失率_4次']:
            datadf[i] = datadf.apply(column_percent_format, axis=1, col=i, dec=1)
    file_path = write_excels_one_sheet(file_name, [1, 1], data=datadf)
    my_sender = emailSend(users=user_lst, title=file_name, file_path=file_path)
    my_sender.sender()


@logger_in_file('reportlog.log')
def goods_stock_week_report(user_lst):
    """
    商品库存信息、价格、评论信息
    :param user_lst: 接受邮件的用户列表
    :return: None
    """
    file_name = '商品库存报表'
    data0 = run_sql(sql_file.goods_stock_week_report())
    datas = [{'data': [(data0, 'data')]}]
    file_path = data_to_excel(filename=file_name, data_list=datas)
    my_sender = emailSend(users=user_lst, title=file_name, file_path=file_path)
    my_sender.sender()


@logger_in_file('reportlog.log')
def goods_sale_rate_week_email(user_lst):
    """
    三级类目商品动销率
    :param user_lst: 接收人
    :return: None
    """
    file_name = '近8周类目动销率'
    datadf = run_sql(sql_file.goods_sale_rate_week())
    new_df = datadf.pivot_table(index=['类目', '一级', '二级', '三级'], columns='周', values=['在架款数', '销售款数'],
                                fill_value=0).reset_index()
    for i in range(1, 9):
        new_df[('动销率', '前{n}周'.format(n=i))] = new_df[('销售款数', '前{n}周'.format(n=i))] / new_df[
            ('在架款数', '前{n}周'.format(n=i))]
        new_df[('动销率', '前{n}周'.format(n=i))] = \
            new_df[('动销率', '前{n}周'.format(n=i))].apply(lambda x: format(x, '.2%'))
    file_path = write_excels_one_sheet(file_name, [1, 1], data=new_df)
    my_sender = emailSend(users=user_lst, title='近8周类目动销率', file_path=file_path)
    my_sender.sender()


@logger_in_file('reportlog.log')
def cate_level1_day_report(user_lst: list, category: str):
    """
    类目日报
    :param user_lst: 接收邮件的用户列表
    :param category: 类目
    :return: None
    """
    msg = sql_file.new_goods_no_pop_sql(category=category)
    datadf = run_sql(msg)
    datadf['data_date'] = pd.to_datetime(datadf['data_date'], format="%Y-%m-%d")
    datadf = datadf.sort_values(by=['cate_level1_name', 'data_date'], ascending=[True, True])

    def gmv_all():
        """计算类目GMV同比、环比、本月毛利率"""
        new_df = datadf[['data_date', 'cate_level1_name', 'revenue', 'cost_with_vat']]
        last_month_df = \
            new_df[(new_df['data_date'] >= last_month_first_day) & (new_df['data_date'] < this_month_first_day)][
                ['cate_level1_name', 'revenue']]
        this_month_df_yoy = \
            new_df[(new_df['data_date'] >= this_month_first_day_yoy) & (new_df['data_date'] <= yesterday_yoy)][
                ['cate_level1_name', 'revenue']]
        this_month_df = new_df[(new_df['data_date'] >= this_month_first_day) & (new_df['data_date'] <= yesterday)][
            ['cate_level1_name', 'revenue', 'cost_with_vat']]

        last_month_df_re = last_month_df.groupby(['cate_level1_name']).sum().reset_index()
        last_month_df_re.columns = ['cate_level1_name', 'last_month_revenue']
        this_month_df_yoy_re = this_month_df_yoy.groupby('cate_level1_name').sum().reset_index()
        this_month_df_yoy_re.columns = ['cate_level1_name', 'this_month_yoy_revenue']
        this_month_df_re = this_month_df.groupby('cate_level1_name').sum().reset_index()
        this_month_df_re.columns = ['cate_level1_name', 'this_month_revenue', 'this_month_cost_with_vat']

        tmp0 = pd.merge(left=this_month_df_re, right=this_month_df_yoy_re, left_on='cate_level1_name',
                        right_on='cate_level1_name', how='left')
        all_df = pd.merge(left=tmp0, right=last_month_df_re, left_on='cate_level1_name', right_on='cate_level1_name',
                          how='left')

        all_df.loc['合计'] = all_df.apply(totle_sum, axis=0)

        all_df['margin'] = 1 - all_df['this_month_cost_with_vat'] / all_df['this_month_revenue']
        all_df['gmv_yoy'] = all_df['this_month_revenue'] / all_df['this_month_yoy_revenue'] - 1
        all_df['gmv_last_rate'] = all_df['this_month_revenue'] / all_df['last_month_revenue'] - 1
        all_df.columns = ['一级', '本月GMV', '本月成本', '去年同期GMV', '上月GMV', '毛利率', 'GMV同比', 'GMV环比']
        all_df = all_df[['一级', '本月GMV', '去年同期GMV', '上月GMV', '毛利率', 'GMV同比', 'GMV环比']]
        all_df['本月GMV'] = all_df.apply(column_format, axis=1, col='本月GMV', dec=0)
        all_df['去年同期GMV'] = all_df.apply(column_format, axis=1, col='去年同期GMV', dec=0)
        all_df['上月GMV'] = all_df.apply(column_format, axis=1, col='上月GMV', dec=0)
        all_df['毛利率'] = all_df.apply(column_percent_format, axis=1, col='毛利率', dec=1)
        all_df['GMV同比'] = all_df.apply(column_percent_format, axis=1, col='GMV同比', dec=1)
        all_df['GMV环比'] = all_df.apply(column_percent_format, axis=1, col='GMV环比', dec=1)
        return all_df

    all_df = gmv_all()
    first_lst = list(all_df['一级'])
    first_lst = first_lst[:-1]
    first_lst.insert(0, 'data_date')

    def cala_gmv():
        """计算每日GMV"""
        last_month_gmv_data = \
            datadf[(datadf['data_date'] >= last_month_first_day) & (datadf['data_date'] < this_month_first_day)][
                ['data_date', 'cate_level1_name', 'revenue']]
        last_month_gmv_data = pd.DataFrame(last_month_gmv_data.groupby('cate_level1_name')['revenue'].sum())
        last_month_gmv_data.columns = [str(today.month - 1) + '月合计']
        last_month_gmv_data = last_month_gmv_data.T
        last_month_gmv_data['data_date'] = '上月合计'
        last_month_gmv_data = last_month_gmv_data[first_lst]

        this_month_gmv_data = \
            datadf[(datadf['data_date'] >= this_month_first_day) & (datadf['data_date'] <= yesterday)][
                ['data_date', 'cate_level1_name', 'revenue']]
        this_month_gmv_df = this_month_gmv_data.pivot_table(columns='cate_level1_name', index='data_date',
                                                            values='revenue', aggfunc='sum').reset_index()
        this_month_gmv_df.loc['合计'] = this_month_gmv_df.apply(totle_sum, axis=0)
        this_month_gmv_df = this_month_gmv_df[first_lst]
        this_year_gmv_return = pd.concat([last_month_gmv_data, this_month_gmv_df], axis=0, sort=True)[first_lst]
        this_year_gmv_return['合计'] = this_year_gmv_return.apply(totle_sum, axis=1)

        for i in this_year_gmv_return.columns:
            if i == 'data_date':
                pass
            else:
                this_year_gmv_return[i] = this_year_gmv_return.apply(column_format, axis=1, col=i, dec=0)
        return this_year_gmv_return

    def cala_nuw_goods():
        """计算每日上新数量"""
        last_month_new_num_data = \
            datadf[(datadf['data_date'] >= last_month_first_day) & (datadf['data_date'] < this_month_first_day)][
                ['data_date', 'cate_level1_name', 'new_up_num']]
        last_month_new_num_data = pd.DataFrame(last_month_new_num_data.groupby('cate_level1_name')['new_up_num'].sum())
        last_month_new_num_data.columns = [str(today.month - 1) + '月合计']
        last_month_new_num_data = last_month_new_num_data.T
        last_month_new_num_data['data_date'] = '上月合计'

        this_month_new_nu_data = \
            datadf[(datadf['data_date'] >= this_month_first_day) & (datadf['data_date'] <= yesterday)][
                ['data_date', 'cate_level1_name', 'new_up_num']]
        this_month_new_nu_df = this_month_new_nu_data.pivot_table(columns='cate_level1_name', index='data_date',
                                                                  values='new_up_num', aggfunc='sum').reset_index()
        this_month_new_nu_df.loc['合计'] = this_month_new_nu_df.apply(totle_sum, axis=0)
        this_month_new_nu_df = this_month_new_nu_df[first_lst]
        this_year_new_nu_return = pd.concat([last_month_new_num_data, this_month_new_nu_df], axis=0, sort=True)[
            first_lst]
        this_year_new_nu_return['合计'] = this_year_new_nu_return.apply(totle_sum, axis=1)
        return this_year_new_nu_return

    def cala_income():
        """计算日净利"""
        if '合计' in first_lst:
            first_lst.remove('合计')
        last_month_gmv_data = \
            datadf[(datadf['data_date'] >= last_month_first_day) & (datadf['data_date'] < this_month_first_day)][
                ['data_date', 'cate_level1_name', 'pre_income']]
        last_month_gmv_data = pd.DataFrame(last_month_gmv_data.groupby('cate_level1_name')['pre_income'].sum())
        last_month_gmv_data.columns = [str(today.month - 1) + '月合计']
        last_month_gmv_data = last_month_gmv_data.T
        last_month_gmv_data['data_date'] = '上月合计'
        last_month_gmv_data = last_month_gmv_data[first_lst]

        this_month_gmv_data = \
            datadf[(datadf['data_date'] >= this_month_first_day) & (datadf['data_date'] <= yesterday)][
                ['data_date', 'cate_level1_name', 'pre_income']]
        this_month_gmv_df = this_month_gmv_data.pivot_table(columns='cate_level1_name', index='data_date',
                                                            values='pre_income', aggfunc='sum').reset_index()
        this_month_gmv_df.loc['合计'] = this_month_gmv_df.apply(totle_sum, axis=0)
        this_month_gmv_df = this_month_gmv_df[first_lst]
        this_year_gmv_return = pd.concat([last_month_gmv_data, this_month_gmv_df], axis=0, sort=True)[first_lst]
        this_year_gmv_return['合计'] = this_year_gmv_return.apply(totle_sum, axis=1)

        for i in this_year_gmv_return.columns:
            if i == 'data_date':
                pass
            else:
                this_year_gmv_return[i] = this_year_gmv_return.apply(column_format, axis=1, col=i, dec=0)
        return this_year_gmv_return

    def cala_margin():
        """计算每日毛利率"""
        first_lst2 = first_lst
        last_month = \
            datadf[(datadf['data_date'] >= last_month_first_day) & (datadf['data_date'] < this_month_first_day)][
                ['data_date', 'cate_level1_name', 'revenue', 'cost_with_vat']]
        last_month_gmv_totle = pd.DataFrame(last_month.groupby('cate_level1_name')['revenue'].sum())
        last_month_cost_totle = pd.DataFrame(last_month.groupby('cate_level1_name')['cost_with_vat'].sum())
        last_month_margin = pd.concat([last_month_gmv_totle, last_month_cost_totle], axis=1)
        last_month_margin.loc['合计'] = last_month_margin.apply(totle_sum, axis=0)
        last_month_margin['margin'] = 1 - last_month_margin['cost_with_vat'] / last_month_margin['revenue']
        last_month_margin.loc['合计', 'margin'] = 1 - last_month_margin.loc['合计', 'cost_with_vat'] / \
                                                last_month_margin.loc['合计', 'revenue']
        last_month_margin_df = pd.DataFrame(last_month_margin['margin'])
        last_month_margin_df = last_month_margin_df.T
        last_month_margin_df['data_date'] = '上月合计'
        first_lst2.append('合计')
        last_month_margin_df = last_month_margin_df[first_lst2]

        this_month_df = datadf[(datadf['data_date'] >= this_month_first_day) & (datadf['data_date'] <= yesterday)][
            ['data_date', 'cate_level1_name', 'revenue', 'cost_with_vat']]
        this_month_margin = this_month_df.pivot_table(columns='cate_level1_name', index='data_date',
                                                      values=['revenue', 'cost_with_vat'], aggfunc='sum').reset_index()
        # print(this_month_margin)
        margin_gmv_lst = []
        margin_cost_lst = []
        # print(first_lst)
        first_lst2 = first_lst
        for i in first_lst2[1:-1]:
            margin_gmv_lst.append(('revenue', i))
            margin_cost_lst.append(('cost_with_vat', i))
        this_month_margin[('revenue', '合计')] = this_month_margin[margin_gmv_lst].apply(sum, axis=1)
        this_month_margin[('cost_with_vat', '合计')] = this_month_margin[margin_cost_lst].apply(sum, axis=1)
        this_month_margin.loc['合计'] = this_month_margin.apply(totle_sum, axis=0)
        colums_lst = [i[1] for i in this_month_margin.columns]
        colums_lst.remove('')
        colums_lst = list(set(colums_lst))
        for i in colums_lst:
            this_month_margin[('margin', i)] = 1 - this_month_margin[('cost_with_vat', i)] / this_month_margin[
                ('revenue', i)]
        new_columns_tmp = [('data_date', '')]
        new_columns_lst = ['data_date']
        for i in this_month_margin.columns:
            if i[0] == 'margin':
                new_columns_tmp.append(i)
                new_columns_lst.append(i[1])
        this_month_margin_df = this_month_margin[new_columns_tmp]
        this_month_margin_df.columns = new_columns_lst
        this_month_margin_df = this_month_margin_df[first_lst2]
        this_year_margin_df = pd.concat([last_month_margin_df, this_month_margin_df], axis=0)

        this_year_margin_df.loc['合计', 'data_date'] = '本月合计'
        this_year_margin_df.index = [i for i in range(len(this_year_margin_df))]
        for i in this_year_margin_df.columns:
            if i == 'data_date':
                pass
            else:
                this_year_margin_df[i] = this_year_margin_df.apply(column_percent_format, axis=1, col=i, dec=1)
        return this_year_margin_df[first_lst]

    this_year_gmv_df = cala_gmv()
    this_year_new_num_df = cala_nuw_goods()
    this_year_margin_df = cala_margin()
    # this_year_income_df = cala_income()

    print("数据保存")
    file_path = write_excels_one_sheet('{cate}日报'.format(cate=category), [5, 1],
                                       all_gmv=all_df,
                                       this_year_gmv=this_year_gmv_df,
                                       this_year_new_goods=this_year_new_num_df,
                                       this_year_margin=this_year_margin_df,
                                       #    this_year_income=this_year_income_df
                                       )
    print("邮件发送")
    my_sender = emailSend(users=user_lst, title='{cate}类目日报'.format(cate=category), file_path=file_path)
    my_sender.sender()


@logger_in_file('reportlog.log')
def cate_level1_month_report(user_lst: list):
    """
    一级类目日报，包括GMV、同比、毛利、在架款数、款均销量、件均价
    :param user_lst: 接收用户列表
    :return: None
    """
    msg = sql_file.cate_level1_month()
    datadf = run_sql(msg)
    cate_lst = set(datadf['category_group'])

    def calc_this_year_gmv():
        """计算今年一级类目GMV"""
        df_lst = []
        for i in cate_lst:
            gmvdf = datadf[(datadf['years0'] == str(today.year)) & (datadf['category_group'] == i)]
            cat0 = gmvdf.pivot_table(columns="year_month", index=['category_group', 'cate_level1_name'],
                                     values="revenue", aggfunc='sum').reset_index()
            cat0 = cat0.fillna(value=0)
            cat0.loc['合计'] = cat0.apply(totle_sum, axis=0)
            cat0['合计'] = cat0.apply(totle_sum, axis=1)
            cat0.loc['合计', 'category_group'] = i + '合计'
            df_lst.append(cat0)
        re_gmv_df = pd.concat(df_lst).reset_index()
        cols_lst = list(re_gmv_df.columns)[1:]
        re_gmv_df = re_gmv_df[cols_lst]
        for i in re_gmv_df.columns:
            if i in ['category_group', 'cate_level1_name']:
                pass
            else:
                re_gmv_df[i] = re_gmv_df.apply(column_format, axis=1, col=i, dec=0)
        return re_gmv_df

    def calc_gmv_yty_rate():
        """计算一级类目GMV同比"""
        df_lst = []
        for itm in cate_lst:
            gmv_yty = datadf[((datadf['years0'] == str(today.year)) | (datadf['years0'] == str(today.year - 1))) & (
                    datadf['category_group'] == itm)]
            cat0 = gmv_yty.pivot_table(columns=['years0', 'year_month'], index=['category_group', 'cate_level1_name'],
                                       values='revenue', aggfunc='sum').reset_index()
            cat0 = cat0.fillna(value=0)
            this_year_gmv = []
            last_year_gmv = []
            for i in cat0.columns:
                if i[1] != '' and i[0] == str(today.year - 1):
                    last_year_gmv.append(i)
                elif i[1] != '' and i[0] == str(today.year):
                    this_year_gmv.append(i)
            cat0[(str(today.year - 1), '合计')] = cat0[last_year_gmv].apply(sum, axis=1)
            cat0[(str(today.year), '合计')] = cat0[this_year_gmv].apply(sum, axis=1)
            cat0.loc['合计'] = cat0.apply(totle_sum, axis=0)
            for i in cat0.columns:
                if i[1] == '' or i[0] == str(today.year - 1):
                    pass
                elif i[1] != '合计':
                    last_year = str(today.year - 1)
                    last_month = str(today.year - 1) + i[1][-2:]
                    cat0[('同比', i[1])] = cat0[i] / cat0[(last_year, last_month)] - 1
                elif i[1] == '合计':
                    cat0[('同比', i[1])] = cat0[i] / cat0[(last_year, '合计')] - 1
            tmp_col_lst = [('category_group', ''), ('cate_level1_name', '')]
            new_df_lst = ['category_group', 'cate_level1_name']
            for i in cat0.columns:
                if i[0] == '同比':
                    tmp_col_lst.append(i)
                    new_df_lst.append(i[1])
            yty_df = cat0[tmp_col_lst]
            yty_df = yty_df.fillna(value=0)
            yty_df.columns = new_df_lst
            for i in yty_df.columns:
                if i in ['category_group', 'cate_level1_name']:
                    pass
                else:
                    yty_df[i] = yty_df.apply(column_percent_format, axis=1, col=i, dec=1)
            yty_df.loc['合计', 'category_group'] = itm + '合计'
            df_lst.append(yty_df)
        re_yty_df = pd.concat(df_lst).reset_index()
        re_yty_df = re_yty_df[re_yty_df.columns[1:]]
        return re_yty_df

    def calc_this_year_margin():
        """计算今年一级类目毛利"""
        df_lst = []
        for itm in cate_lst:
            margin = datadf[(datadf['years0'] == str(today.year)) & (datadf['category_group'] == itm)]
            cat0 = margin.pivot_table(columns='year_month', index=['category_group', 'cate_level1_name'],
                                      values=['revenue', 'cost_with_vat'], aggfunc='sum').reset_index()
            cat0 = cat0.fillna(value=0)
            gmv_lst = []
            cost_lst = []
            for i in cat0.columns:
                if i[1] != '' and i[0] == 'revenue':
                    gmv_lst.append(i)
                elif i[1] != '' and i[0] == 'cost_with_vat':
                    cost_lst.append(i)
            cat0[('revenue', '合计')] = cat0[gmv_lst].apply(sum, axis=1)
            cat0[('cost_with_vat', '合计')] = cat0[cost_lst].apply(sum, axis=1)
            cat0.loc['合计'] = cat0.apply(totle_sum, axis=0)
            for i in cat0.columns:
                if i[1] == '' or i[0] == 'cost_with_vat':
                    pass
                elif i[1] != '合计':
                    cat0[('毛利率', i[1])] = 1 - cat0[('cost_with_vat', i[1])] / cat0[i]
                elif i[1] == '合计':
                    cat0[('毛利率', i[1])] = 1 - cat0[('cost_with_vat', '合计')] / cat0[i]
            tmp_col_lst = [('category_group', ''), ('cate_level1_name', '')]
            new_df_lst = ['category_group', 'cate_level1_name']
            for i in cat0.columns:
                if i[0] == '毛利率':
                    tmp_col_lst.append(i)
                    new_df_lst.append(i[1])
            margin_df = cat0[tmp_col_lst]
            margin_df = margin_df.fillna(value=0)
            margin_df.columns = new_df_lst
            for i in margin_df.columns:
                if i in ['category_group', 'cate_level1_name']:
                    pass
                else:
                    margin_df[i] = margin_df.apply(column_percent_format, axis=1, col=i, dec=1)
            margin_df.loc['合计', 'category_group'] = itm + '合计'
            df_lst.append(margin_df)
        re_margin_df = pd.concat(df_lst).reset_index()
        re_margin_df = re_margin_df[re_margin_df.columns[1:]]
        return re_margin_df

    def calc_unit_price():
        """计算今年一级类目件均价"""
        df_lst = []
        for itm in cate_lst:
            margin = datadf[(datadf['years0'] == str(today.year)) & (datadf['category_group'] == itm)]
            cat0 = margin.pivot_table(columns='year_month', index=['category_group', 'cate_level1_name'],
                                      values=['revenue', 'num'], aggfunc='sum').reset_index()
            cat0 = cat0.fillna(value=0)
            gmv_lst = []
            num_lst = []
            for i in cat0.columns:
                if i[1] != '' and i[0] == 'revenue':
                    gmv_lst.append(i)
                elif i[1] != '' and i[0] == 'num':
                    num_lst.append(i)
            cat0[('revenue', '合计')] = cat0[gmv_lst].apply(sum, axis=1)
            cat0[('num', '合计')] = cat0[num_lst].apply(sum, axis=1)
            cat0.loc['合计'] = cat0.apply(totle_sum, axis=0)
            for i in cat0.columns:
                if i[1] == '' or i[0] == 'num':
                    pass
                elif i[1] != '合计':
                    cat0[('件均价', i[1])] = cat0[i] / cat0[('num', i[1])]
                elif i[1] == '合计':
                    cat0[('件均价', i[1])] = cat0[i] / cat0[('num', '合计')]
            tmp_col_lst = [('category_group', ''), ('cate_level1_name', '')]
            new_df_lst = ['category_group', 'cate_level1_name']
            for i in cat0.columns:
                if i[0] == '件均价':
                    tmp_col_lst.append(i)
                    new_df_lst.append(i[1])
            margin_df = cat0[tmp_col_lst]
            margin_df = margin_df.fillna(value=0)
            margin_df.columns = new_df_lst
            for i in margin_df.columns:
                if i in ['category_group', 'cate_level1_name']:
                    pass
                else:
                    margin_df[i] = margin_df.apply(column_format, axis=1, col=i, dec=2)
            margin_df.loc['合计', 'category_group'] = itm + '合计'
            df_lst.append(margin_df)
        re_unit_price_df = pd.concat(df_lst).reset_index()
        re_unit_price_df = re_unit_price_df[re_unit_price_df.columns[1:]]
        return re_unit_price_df

    def calc_on_goods_num():
        """计算今年一级类目在架款数"""
        df_lst = []
        for itm in cate_lst:
            on_goods_num = datadf[(datadf['years0'] == str(today.year)) & (datadf['category_group'] == itm)]
            cat0 = on_goods_num.pivot_table(columns="year_month", index=['category_group', 'cate_level1_name'],
                                            values="goods_num", aggfunc='sum').reset_index()
            cat0 = cat0.fillna(value=0)
            cat0.loc['合计'] = cat0.apply(totle_sum, axis=0)
            cat0['合计'] = cat0.apply(totle_sum, axis=1)
            cat0.loc['合计', 'category_group'] = itm + '合计'
            df_lst.append(cat0)
        re_on_goods_df = pd.concat(df_lst).reset_index()
        cols_lst = list(re_on_goods_df.columns)[1:]
        re_on_goods_df = re_on_goods_df[cols_lst]
        for i in re_on_goods_df.columns:
            if i in ['category_group', 'cate_level1_name']:
                pass
            else:
                re_on_goods_df[i] = re_on_goods_df.apply(column_format, axis=1, col=i, dec=0)
        return re_on_goods_df

    def calc_unit_goods_sale_num():
        """"计算今年一级类目款均销量"""
        df_lst = []
        for itm in cate_lst:
            margin = datadf[(datadf['years0'] == str(today.year)) & (datadf['category_group'] == itm)]
            cat0 = margin.pivot_table(columns='year_month', index=['category_group', 'cate_level1_name'],
                                      values=['goods_num', 'num'], aggfunc='sum').reset_index()
            cat0 = cat0.fillna(value=0)
            on_goods_lst = []
            num_lst = []
            for i in cat0.columns:
                if i[1] != '' and i[0] == 'goods_num':
                    on_goods_lst.append(i)
                elif i[1] != '' and i[0] == 'num':
                    num_lst.append(i)
            cat0[('goods_num', '合计')] = cat0[on_goods_lst].apply(sum, axis=1)
            cat0[('num', '合计')] = cat0[num_lst].apply(sum, axis=1)
            cat0.loc['合计'] = cat0.apply(totle_sum, axis=0)
            for i in cat0.columns:
                if i[1] == '' or i[0] == 'num':
                    pass
                elif i[1] != '合计':
                    cat0[('款均销量', i[1])] = cat0[('num', i[1])] / cat0[i]
                elif i[1] == '合计':
                    cat0[('款均销量', i[1])] = cat0[('num', '合计')] / cat0[i]
            tmp_col_lst = [('category_group', ''), ('cate_level1_name', '')]
            new_df_lst = ['category_group', 'cate_level1_name']
            for i in cat0.columns:
                if i[0] == '款均销量':
                    tmp_col_lst.append(i)
                    new_df_lst.append(i[1])
            unit_goods_num_df = cat0[tmp_col_lst]
            unit_goods_num_df = unit_goods_num_df.fillna(value=0)
            unit_goods_num_df.columns = new_df_lst
            for i in unit_goods_num_df.columns:
                if i in ['category_group', 'cate_level1_name']:
                    pass
                else:
                    unit_goods_num_df[i] = unit_goods_num_df.apply(column_format, axis=1, col=i, dec=2)
            unit_goods_num_df.loc['合计', 'category_group'] = itm + '合计'
            df_lst.append(unit_goods_num_df)
        re_on_goods_df = pd.concat(df_lst).reset_index()
        re_on_goods_df = re_on_goods_df[re_on_goods_df.columns[1:]]
        return re_on_goods_df

    this_year_gmv = calc_this_year_gmv()
    gmv_yty_rate = calc_gmv_yty_rate()
    this_year_margin = calc_this_year_margin()
    on_goods_num = calc_on_goods_num()
    unit_goods_num = calc_unit_goods_sale_num()
    unit_price = calc_unit_price()

    file_path = write_excels_one_sheet('一级类目月报', [6, 1],
                                       GMV=this_year_gmv,
                                       GMV同比=gmv_yty_rate,
                                       毛利率=this_year_margin,
                                       在架款数=on_goods_num,
                                       款均销量=unit_goods_num,
                                       件单价=unit_price,
                                       )
    my_sender = emailSend(users=user_lst, title='一级类目月报', file_path=file_path)
    my_sender.sender()


def new_supp_week_report(user_lst: list):
    """
    新合作供应商周报
    :param user_lst:
    :return:
    """
    msg = sql_file.new_supp_week()
    datadf = run_sql(msg)
    file_path = write_excels_one_sheet('新合作供应商', [2, 1],
                                       新合作供应商=datadf
                                       )
    my_sender = emailSend(users=user_lst, title='本周新合作供应商', file_path=file_path)
    my_sender.sender()


def quarter_goods_day_report(user_lst: list):
    """
    买断商品、当季商品库存跟踪
    :param user_lst: 接收邮件用户列表
    :return: None
    """
    datadf = run_sql(sql_file.quarter_goods())
    file_path = write_excels_one_sheet('当季商品&买断商品库存变动表', [2, 1],
                                       商品表=datadf
                                       )
    my_sender = emailSend(users=user_lst, title='当季商品&买断商品库存变动表', file_path=file_path)
    my_sender.sender()


def depot_stock_goods_report(user_lst: list):
    """
    库存日报表
    :param user_lst: 接收邮件用户列表
    :return: None
    """
    goods_df = run_sql(sql_file.depot_stock_goods())
    goods_stock = goods_df[['类目', '是否在架', '自由库存', 'sa自由库存', 'uae自由库存', '昨天销量',
                            '近7天销量', '近30天销量', '昨天gmv', '近7天gmv', '近30天gmv']].groupby('类目').sum().reset_index()
    goods_stock.loc['合计'] = goods_stock.apply(totle_sum, axis=0)
    goods_stock['昨天gmv'] = goods_stock.apply(column_format, axis=1, col='昨天gmv', dec=0)
    goods_stock['近7天gmv'] = goods_stock.apply(column_format, axis=1, col='近7天gmv', dec=0)
    goods_stock['近30天gmv'] = goods_stock.apply(column_format, axis=1, col='近30天gmv', dec=0)
    html_df = goods_stock.to_html(classes='mystyle')
    html_str = """
    <html>
    <head><title>HTML Pandas Dataframe with CSS</title></head>
    <link rel="stylesheet" type="text/css" href="F:/pro/JollychicEmail/static/css/df_html_style.css"/>
    <body>
        {table}
    </body>
    </html>
    """.format(table=html_df)

    file_path = write_excels_one_sheet('商品库存表', [2, 1],
                                       商品表=goods_df
                                       )
    my_sender = emailSend(users=user_lst, title='商品库存表', file_path=file_path)
    my_sender.sender(html_df)


def create_goods(user_lst: list):
    data0 = run_sql(sql_file.goods_create_2020())
    datas = [{'no_slae': [(data0, 'data')]}]

    file_path = data_to_excel(filename='开发款流量转化率', data_list=datas)
    my_sender = emailSend(users=user_lst, title='开发款流量转化率', file_path=file_path)
    my_sender.sender()


def goods_msg_tmp():
    data0 = run_sql(sql_file.goods_msg_tmp())
    datas = [{'goods': [(data0, 'data')]}]

    data_to_excel(filename='商品sku价格-婴童时尚', data_list=datas)


def email_test(user_lst, content=None):
    my_sender = emailSend(users=user_lst, title='打我')
    my_sender.sender(content=content)


def day_stock_english_send(user_list):
    """库存表-英文表头"""
    data0 = run_sql(sql_file.day_stock_msg_english())
    datas = [{'goods_stock': [(data0, 'data')]}]

    file_path = data_to_excel(filename='goods_stock', data_list=datas)
    my_sender = emailSend(users=user_list, title='goods_stock', file_path=file_path)
    my_sender.sender()


def goods_view_send(user_list):
    """发送商品视频数据"""
    data0 = run_sql(sql_file.goods_view())
    datas = [{'goods_view': [(data0, 'data')]}]

    file_path = data_to_excel(filename='goods_view', data_list=datas)
    my_sender = emailSend(users=user_list, title='goods_view', file_path=file_path)
    my_sender.sender()


def goods_stock_send(user_list):
    data0 = run_sql(sql_file.goods_stock_data())
    datas = [{'goods_stock': [(data0, 'data')]}]

    file_path = data_to_excel(filename='goods_stock', data_list=datas)
    my_sender = emailSend(users=user_list, title='goods_stock', file_path=file_path)
    my_sender.sender()


def sku_stock_send(user_list):
    data0 = run_sql(sql_file.sku_stock_data())
    datas = [{'sku_stock': [(data0, 'data')]}]

    file_path = data_to_excel(filename='sku_stock', data_list=datas)
    my_sender = emailSend(users=user_list, title='sku_stock', file_path=file_path)
    my_sender.sender()


def tmp_send(user_list, rn_begin, rn_end):
    data0 = run_sql(sql_file.sku_tmp(rn_begin, rn_end))
    datas = [{'goods_tmp': [(data0, 'data')]}]

    file_path = data_to_excel(filename='goods_tmp_kids_{begin}_{end}'.format(begin=rn_begin, end=rn_end),
                              data_list=datas)
    # my_sender = emailSend(users=user_list, title='goods_tmp', file_path=file_path)
    # my_sender.sender()


def provider_2019_month_tmp(user_list):
    data0 = run_sql(sql_file.provider_2019_month_tmp())
    datas = [{'goods_tmp': [(data0, 'data')]}]

    file_path = data_to_excel(filename='goods_tmp', data_list=datas)
    my_sender = emailSend(users=user_list, title='goods_tmp', file_path=file_path)
    my_sender.sender()


def goods_tmp_lyy_send(user_list):
    data0 = run_sql(sql_file.goods_tmp_lyy())
    datas = [{'goods_tmp': [(data0, 'data')]}]

    file_path = data_to_excel(filename='goods_tmp', data_list=datas)
    my_sender = emailSend(users=user_list, title='goods_tmp', file_path=file_path)
    my_sender.sender()


def goods_sku_comment_return_tmp(user_list, category_group):
    import os
    data0 = run_sql(sql_file.goods_sku_comment_return(category_group))
    datas = [{'goods_tmp': [(data0, 'data')]}]
    # file_name = 'sku_tmp_{cat}.csv'.format(cat=category_group)
    # father_dirname = "F:\\pro\\JollychicEmail\\excel_file\\"
    # file_path = os.path.join(father_dirname, file_name)
    # data0.to_csv(file_path)

    file_path = data_to_excel(filename='sku_tmp_{cat}'.format(cat=category_group), data_list=datas)
    my_sender = emailSend(users=user_list, title='sku_tmp_{cat}'.format(cat=category_group), file_path=file_path)
    my_sender.sender()


def get_stock_inprice_data():
    data0 = run_sql(sql_file.stock_inprice())
    datas = [{'stock_inprice': [(data0, 'data')]}]

    file_path = data_to_excel(filename='stock_inprice', data_list=datas)


def get_new_goods_sale_data(user_list):
    """新品销售情况"""
    data0 = run_sql(sql_file.new_goods_sale_sql())
    datas = [{'new_goods_sale': [(data0, 'data')]}]

    file_path = data_to_excel(filename='新品销售情况', data_list=datas)
    my_sender = emailSend(users=user_list, title='新品销售情况', file_path=file_path)
    msg = """
    新品销售情况
    1. 7天为一周，而不是以自然周切分
    2. 近45天上新的商品判定为新品
    """
    my_sender.sender(content=msg)


def get_gmv_day_report_data(user_list):
    data0 = run_sql(sql_file.gmv_day_report_sql())
    datas = [{'new_data': [(data0, 'data')]}]

    file_path = data_to_excel(filename='日报-gmv-data', data_list=datas)
    my_sender = emailSend(users=user_list, title='日报-gmv-data', file_path=file_path)
    my_sender.sender()


def get_onsale_goods_data(user_list):
    data0 = run_sql(sql_file.on_sale_goods_day_sql())
    datas = [{'日报-在架数': [(data0, 'data')]}]

    file_path = data_to_excel(filename='日报-在架数', data_list=datas)
    my_sender = emailSend(users=user_list, title='日报-在架数', file_path=file_path)
    my_sender.sender()


def get_new_goods_data(user_list):
    data0 = run_sql(sql_file.new_goods_day_sql())
    datas = [{'日报-上新': [(data0, 'data')]}]

    file_path = data_to_excel(filename='日报-上新', data_list=datas)
    my_sender = emailSend(users=user_list, title='日报-上新', file_path=file_path)
    msg = """
    每日上新情况\r\n
    1. test\r\n
    2. test
    """
    my_sender.sender(content=msg)


def tar_goods_price_gmv_duibi_email(user_list):
    data0 = run_sql(sql_file.tar_goods_price_gmv_duibi())
    datas = [{'data': [(data0, 'data')]}]

    file_path = data_to_excel(filename='商品价格对比', data_list=datas)
    my_sender = emailSend(users=user_list, title='商品价格对比', file_path=file_path)
    my_sender.sender()


def send_supp_off_goods_data(user_list):
    """商家下架商品列表
    取昨天下架的商品
    """
    data0 = run_sql(sql_file.supp_off_goods_sql())
    datas = [{'data': [(data0, 'data')]}]

    file_path = data_to_excel(filename='商家下架商品列表', data_list=datas)
    my_sender = emailSend(users=user_list, title='商家下架商品列表', file_path=file_path)
    my_sender.sender()


def sa_depot_goods_sale_num_send(category, user_list):
    """本地仓近7天发货数量"""
    data0 = run_sql(sql_file.sa_depot_goods_sale_num())
    newdf = data0[data0['category_group'] == category].pivot_table(columns='data_date',
                                                                   index=['category_group', 'cate_level1_name',
                                                                          'goods_id'],
                                                                   values='num',
                                                                   aggfunc=sum).reset_index().fillna(0)
    lst = list(newdf.columns)
    date_lst = lst[3:]
    for i in date_lst:
        newdf[i] = newdf.apply(column_format, axis=1, col=i, dec=0)
    newdf['对比'] = newdf[date_lst[-1]] / newdf[date_lst[0]] - 1
    datas = [{'data': [(newdf, 'data')]}]

    file_path = data_to_excel(filename='近7天本地仓发货数量', data_list=datas)
    my_sender = emailSend(users=user_list, title='近7天本地仓发货数量', file_path=file_path)
    my_sender.sender()


def home_3_sku_sale_send(user_lst: list):
    """
    买断商品、当季商品库存跟踪
    :param user_lst: 接收邮件用户列表
    :return: None
    """
    datadf = run_sql(sql_file.home_3_sku_sale())
    file_path = write_excels_one_sheet('家居3月sku销量', [2, 1],
                                       商品表=datadf
                                       )
    my_sender = emailSend(users=user_lst, title='家居3月sku销量', file_path=file_path)
    my_sender.sender()


def gmv_duibi_send(user_lst: list):
    datadf = run_sql(sql_file.gmv_duibi())
    file_path = write_excels_one_sheet('GMV对比', [2, 1],
                                       商品表=datadf
                                       )
    my_sender = emailSend(users=user_lst, title='GMV对比', file_path=file_path)
    my_sender.sender()


def negative_pre_income_send(user_lst: list):
    datadf = run_sql(sql_file.negative_pre_income())
    file_path = write_excels_one_sheet('负净利商品', [2, 1],
                                       商品表=datadf
                                       )
    my_sender = emailSend(users=user_lst, title='负净利商品', file_path=file_path)
    msg = """
        负净利商品取前7天哪一天的负净利的商品，如6月9号取6月2号那一天的负净利商品
        """
    my_sender.sender(msg)



def GMV_department_6_gmv_tar_rate_end(user_list: list):
    datadf = run_sql(sqlmsg=sql_file.GMV_department_6_gmv_tar_rate())
    targetdf = pd.read_excel(r"C:\Users\WIN7\Desktop\下载文件\目标\department_6.xlsx")
    target_cat1_df = targetdf[targetdf['cate_level2_name'].isnull()]
    target_cat2_df = targetdf[targetdf['cate_level2_name'].notnull()]

    cat2_gmv_df = datadf[['department', 'cate_level1_name', 'cate_level2_name', 'revenue']]. \
        groupby(['department', 'cate_level1_name', 'cate_level2_name']).sum().reset_index()
    cat2_gmv_self_df = datadf[datadf['is_pop'] == 0][
        ['department', 'cate_level1_name', 'cate_level2_name', 'revenue', 'cost_with_vat', 'pre_income']]. \
        groupby(['department', 'cate_level1_name', 'cate_level2_name']).sum().reset_index()
    tar_gmv_cat2 = target_cat2_df.merge(cat2_gmv_df, how='left', left_on=['cate_level1_name', 'cate_level2_name'],
                                        right_on=['cate_level1_name', 'cate_level2_name'])
    tar_gmv_cat2.columns = ['manager', 'cate_level1_name', 'cate_level2_name', 'pre_income_tar', 'gmv_tar',
                            'gmv_self_tar', 'pre_income_rate_tar', 'matgin_tar', 'department', 'revenue_all']
    tar_gmv_cat2 = tar_gmv_cat2.merge(cat2_gmv_self_df, how='left', left_on=['cate_level1_name', 'cate_level2_name'],
                                      right_on=['cate_level1_name', 'cate_level2_name'])
    tar_gmv_cat2.columns = ['manager', 'cate_level1_name', 'cate_level2_name', 'pre_income_tar',
                            'gmv_tar', 'gmv_self_tar', 'pre_income_rate_tar', 'matgin_tar',
                            'department_x', 'revenue_all', 'department_y', 'revenue_self',
                            'cost_with_vat_self', 'pre_income_self']
    tar_gmv_cat2 = tar_gmv_cat2[['manager', 'cate_level1_name', 'cate_level2_name', 'pre_income_tar',
                                 'pre_income_self', 'gmv_tar', 'gmv_self_tar', 'pre_income_rate_tar',
                                 'matgin_tar', 'revenue_all', 'revenue_self', 'cost_with_vat_self']]

    cat1_gmv_df = datadf[['department', 'cate_level1_name', 'revenue']]. \
        groupby(['department', 'cate_level1_name']).sum().reset_index()
    cat1_gmv_self_df = datadf[datadf['is_pop'] == 0][
        ['department', 'cate_level1_name', 'revenue', 'cost_with_vat', 'pre_income']]. \
        groupby(['department', 'cate_level1_name']).sum().reset_index()
    tar_gmv_cat1 = target_cat1_df.merge(cat1_gmv_df, how='left', left_on='cate_level1_name',
                                        right_on='cate_level1_name')
    tar_gmv_cat1.columns = ['manager', 'cate_level1_name', 'cate_level2_name', 'pre_income_tar', 'gmv_tar',
                            'gmv_self_tar', 'pre_income_rate_tar', 'matgin_tar', 'department', 'revenue_all']
    tar_gmv_cat1 = tar_gmv_cat1.merge(cat1_gmv_self_df, how='left', left_on='cate_level1_name',
                                      right_on='cate_level1_name')
    tar_gmv_cat1.columns = ['manager', 'cate_level1_name', 'cate_level2_name', 'pre_income_tar',
                            'gmv_tar', 'gmv_self_tar', 'pre_income_rate_tar', 'matgin_tar',
                            'department_x', 'revenue_all', 'department_y', 'revenue_self',
                            'cost_with_vat_self', 'pre_income_self']
    tar_gmv_cat1 = tar_gmv_cat1[['manager', 'cate_level1_name', 'cate_level2_name', 'pre_income_tar',
                                 'pre_income_self', 'gmv_tar', 'gmv_self_tar', 'pre_income_rate_tar',
                                 'matgin_tar', 'revenue_all', 'revenue_self', 'cost_with_vat_self']]

    tar_gmv_df = pd.concat([tar_gmv_cat2, tar_gmv_cat1], axis=0, sort=True)[
        ['manager', 'cate_level1_name', 'cate_level2_name',
         'gmv_tar', 'revenue_all', 'gmv_self_tar', 'revenue_self',
         'pre_income_tar', 'pre_income_self', 'pre_income_rate_tar',
         'matgin_tar', 'cost_with_vat_self']]

    managerdf = tar_gmv_df[['manager', 'gmv_tar', 'revenue_all',
                            'gmv_self_tar', 'revenue_self', 'pre_income_tar',
                            'pre_income_self', 'cost_with_vat_self']].groupby('manager').sum().reset_index()
    cat1df = tar_gmv_df[['cate_level1_name', 'gmv_tar', 'revenue_all',
                         'gmv_self_tar', 'revenue_self', 'pre_income_tar',
                         'pre_income_self', 'cost_with_vat_self']].groupby('cate_level1_name').sum().reset_index()

    managerdf['all_gmv_rate'] = managerdf['revenue_all'] / managerdf['gmv_tar']
    managerdf['self_gmv_rate'] = managerdf['revenue_self'] / managerdf['gmv_self_tar']
    managerdf['margin'] = 1 - managerdf['cost_with_vat_self'] / managerdf['revenue_self']
    managerdf['pre_income_rate'] = managerdf['pre_income_self'] / managerdf['pre_income_tar']
    managerdf = managerdf[['manager', 'gmv_tar', 'revenue_all', 'all_gmv_rate', 'gmv_self_tar',
                           'revenue_self', 'self_gmv_rate', 'pre_income_tar', 'pre_income_self', 'pre_income_rate',
                           'margin']]
    managerdf['gmv_tar'] = managerdf.apply(column_format, axis=1, col='gmv_tar', dec=0)
    managerdf['revenue_all'] = managerdf.apply(column_format, axis=1, col='revenue_all', dec=0)
    managerdf['gmv_self_tar'] = managerdf.apply(column_format, axis=1, col='gmv_self_tar', dec=0)
    managerdf['revenue_self'] = managerdf.apply(column_format, axis=1, col='revenue_self', dec=0)
    managerdf['pre_income_self'] = managerdf.apply(column_format, axis=1, col='pre_income_self', dec=0)
    managerdf['all_gmv_rate'] = managerdf.apply(column_percent_format, axis=1, col='all_gmv_rate', dec=1)
    managerdf['self_gmv_rate'] = managerdf.apply(column_percent_format, axis=1, col='self_gmv_rate', dec=1)
    managerdf['pre_income_rate'] = managerdf.apply(column_percent_format, axis=1, col='pre_income_rate', dec=1)
    managerdf['margin'] = managerdf.apply(column_percent_format, axis=1, col='margin', dec=1)
    managerdf.columns = ['负责人', 'GMV目标(含POP)', 'GMV(含POP)', 'GMV达成(含POP)', 'GMV目标(自营)',
                         'GMV(自营)', 'GMV达成(自营)', '净利目标', '净利润',
                         '净利达成', '毛利率']

    cat1df['all_gmv_rate'] = cat1df['revenue_all'] / cat1df['gmv_tar']
    cat1df['self_gmv_rate'] = cat1df['revenue_self'] / cat1df['gmv_self_tar']
    cat1df['margin'] = 1 - cat1df['cost_with_vat_self'] / cat1df['revenue_self']
    cat1df['pre_income_rate'] = cat1df['pre_income_self'] / cat1df['pre_income_tar']
    cat1df = cat1df[['cate_level1_name', 'gmv_tar', 'revenue_all', 'all_gmv_rate', 'gmv_self_tar',
                     'revenue_self', 'self_gmv_rate', 'pre_income_tar', 'pre_income_self', 'pre_income_rate', 'margin']]
    cat1df['gmv_tar'] = cat1df.apply(column_format, axis=1, col='gmv_tar', dec=0)
    cat1df['revenue_all'] = cat1df.apply(column_format, axis=1, col='revenue_all', dec=0)
    cat1df['gmv_self_tar'] = cat1df.apply(column_format, axis=1, col='gmv_self_tar', dec=0)
    cat1df['revenue_self'] = cat1df.apply(column_format, axis=1, col='revenue_self', dec=0)
    cat1df['pre_income_self'] = cat1df.apply(column_format, axis=1, col='pre_income_self', dec=0)
    cat1df['all_gmv_rate'] = cat1df.apply(column_percent_format, axis=1, col='all_gmv_rate', dec=1)
    cat1df['self_gmv_rate'] = cat1df.apply(column_percent_format, axis=1, col='self_gmv_rate', dec=1)
    cat1df['pre_income_rate'] = cat1df.apply(column_percent_format, axis=1, col='pre_income_rate', dec=1)
    cat1df['margin'] = cat1df.apply(column_percent_format, axis=1, col='margin', dec=1)
    cat1df.columns = ['一级类目', 'GMV目标(含POP)', 'GMV(含POP)', 'GMV达成(含POP)', 'GMV目标(自营)',
                      'GMV(自营)', 'GMV达成(自营)', '净利目标', '净利润',
                      '净利达成', '毛利率']

    tar_gmv_cat2['all_gmv_rate'] = tar_gmv_cat2['revenue_all'] / tar_gmv_cat2['gmv_tar']
    tar_gmv_cat2['self_gmv_rate'] = tar_gmv_cat2['revenue_self'] / tar_gmv_cat2['gmv_self_tar']
    tar_gmv_cat2['margin'] = 1 - tar_gmv_cat2['cost_with_vat_self'] / tar_gmv_cat2['revenue_self']
    tar_gmv_cat2['pre_income_tar_rate'] = tar_gmv_cat2['pre_income_self'] / tar_gmv_cat2['pre_income_tar']
    tar_gmv_cat2['pre_income_rate'] = tar_gmv_cat2['pre_income_self'] / tar_gmv_cat2['revenue_all']
    tar_gmv_cat2 = tar_gmv_cat2[['manager', 'cate_level1_name', 'cate_level2_name', 'gmv_tar',
                                 'revenue_all', 'all_gmv_rate', 'gmv_self_tar', 'revenue_self',
                                 'self_gmv_rate', 'pre_income_tar', 'pre_income_self', 'pre_income_tar_rate',
                                 'pre_income_rate_tar', 'pre_income_rate', 'matgin_tar', 'margin']]
    tar_gmv_cat2['gmv_tar'] = tar_gmv_cat2.apply(column_format, axis=1, col='gmv_tar', dec=0)
    tar_gmv_cat2['revenue_all'] = tar_gmv_cat2.apply(column_format, axis=1, col='revenue_all', dec=0)
    tar_gmv_cat2['gmv_self_tar'] = tar_gmv_cat2.apply(column_format, axis=1, col='gmv_self_tar', dec=0)
    tar_gmv_cat2['revenue_self'] = tar_gmv_cat2.apply(column_format, axis=1, col='revenue_self', dec=0)
    tar_gmv_cat2['pre_income_self'] = tar_gmv_cat2.apply(column_format, axis=1, col='pre_income_self', dec=0)
    tar_gmv_cat2['all_gmv_rate'] = tar_gmv_cat2.apply(column_percent_format, axis=1, col='all_gmv_rate', dec=1)
    tar_gmv_cat2['self_gmv_rate'] = tar_gmv_cat2.apply(column_percent_format, axis=1, col='self_gmv_rate', dec=1)
    tar_gmv_cat2['pre_income_tar_rate'] = tar_gmv_cat2.apply(column_percent_format, axis=1, col='pre_income_tar_rate',
                                                             dec=1)
    tar_gmv_cat2['pre_income_rate_tar'] = tar_gmv_cat2.apply(column_percent_format, axis=1, col='pre_income_rate_tar',
                                                             dec=1)
    tar_gmv_cat2['pre_income_rate'] = tar_gmv_cat2.apply(column_percent_format, axis=1, col='pre_income_rate', dec=1)
    tar_gmv_cat2['matgin_tar'] = tar_gmv_cat2.apply(column_percent_format, axis=1, col='matgin_tar', dec=1)
    tar_gmv_cat2['margin'] = tar_gmv_cat2.apply(column_percent_format, axis=1, col='margin', dec=1)
    tar_gmv_cat2.columns = ['负责人', '一级类目', '二级类目', 'GMV目标(含POP)',
                            'GMV(含POP)', 'GMV达成(含POP)', 'GMV目标(自营)', 'GMV(自营)',
                            'GMV达成(自营)', '净利目标', '净利润',
                            '净利达成', '净利率目标', '净利率',
                            '毛利率目标', '毛利率']
    # tar_gmv_cat2  cat1df  managerdf

    print("数据保存")
    file_path = write_excels_one_sheet('六部达成日报', [3, 1],
                                       managerdf=managerdf,
                                       cat1df=cat1df,
                                       tar_gmv_cat2=tar_gmv_df,
                                       )
    print("邮件发送")
    my_sender = emailSend(users=user_list, title='六部达成日报', file_path=file_path)
    my_sender.sender()


def stock_zhangtianjian_send(user_lst: list):
    datadf = run_sql(sql_file.stock_tianjian())
    file_path = write_excels_one_sheet('男装转化030405', [2, 1],
                                       商品表=datadf
                                       )
    my_sender = emailSend(users=user_lst, title='男装转化030405', file_path=file_path)
    my_sender.sender()


def flash_week_home_send(user_lst: list):
    datadf = run_sql(sql_file.flash_week_home())
    file_path = write_excels_one_sheet('家居闪购data', [2, 1],
                                       商品表=datadf
                                       )
    my_sender = emailSend(users=user_lst, title='家居闪购data', file_path=file_path)
    my_sender.sender()


def flash_week_kids_send(user_lst: list):
    datadf = run_sql(sql_file.flash_week_kids())
    file_path = write_excels_one_sheet('母婴童闪购data', [2, 1],
                                       商品表=datadf
                                       )
    my_sender = emailSend(users=user_lst, title='母婴童闪购data', file_path=file_path)
    my_sender.sender()


def flash_week_beauty_send(user_lst: list):
    datadf = run_sql(sql_file.flash_week_beauty())
    file_path = write_excels_one_sheet('美妆闪购data', [2, 1],
                                       商品表=datadf
                                       )
    my_sender = emailSend(users=user_lst, title='美妆闪购data', file_path=file_path)
    my_sender.sender()



def function_afresh():
    """报表补发"""
    cate_level1_day_report(user_lst=['long.long@jollycorp.com',
                                     'business-4th@jollycorp.com'], category='家居')
    cate_level1_day_report(user_lst=['long.long@jollycorp.com',
                                     'james.hong@jollycorp.com',
                                     'rita.liang@jollycorp.com',
                                     'ailsa@jollycorp.com',
                                     'laine@jollycorp.com'], category='beauty')
    cate_level1_day_report(user_lst=['long.long@jollycorp.com',
                                     "business-5th@jollycorp.com"],
                           category='孕婴童用品')
    cate_level1_day_report(user_lst=['long.long@jollycorp.com',
                                     "business-5th@jollycorp.com"],
                           category='婴童时尚')

    create_goods(['long.long@jollycorp.com',
                  'tracy@jollycorp.com',
                  'yvonne.luo@jollycorp.com',
                  "nova.li@jollycorp.com",
                  'soda@jollycorp.com'])

    depot_stock_goods_report(['home_kids_beauty@jollycorp.com'])

    get_new_goods_sale_data(['long.long@jollycorp.com', 'soda@jollycorp.com'])

    send_supp_off_goods_data(['long.long@jollycorp.com',
                              'michelle.xue@jollycorp.com',
                              'ketty@jollycorp.com',
                              'soda@jollycorp.com'])

    sa_depot_goods_sale_num_send('家居', ['long.long@jollycorp.com',
                                        'lycoris@jollycorp.com',
                                        'ketty@jollycorp.com'])

    get_gmv_day_report_data(['long.long@jollycorp.com', "mango@jollycorp.com"])
    get_onsale_goods_data(['long.long@jollycorp.com'])
    get_new_goods_data(['long.long@jollycorp.com'])
    GMV_department_6_gmv_tar_rate_end(['long.long@jollycorp.com', 'business-6th@jollycorp.com'])

    if today_week == 1:
        goods_view_send(['long.long@jollycorp.com',
                         'rita.liang@jollycorp.com',
                         "business-4th@jollycorp.com"])

        cate_level1_month_report(['long.long@jollycorp.com',
                                  "business-4th@jollycorp.com"])
        print("发送邮件-婴童时尚月报")
        kids_data_goods_report('month', ['long.long@jollycorp.com',
                                         'kidsfashion@jollycorp.com',
                                         'soda@jollycorp.com',
                                         'amy.ge@jollycorp.com'])
        print("发送邮件-婴童时尚四级类目月报")
        kids_data_cate_report('month', ['long.long@jollycorp.com',
                                        'kidsfashion@jollycorp.com',
                                        'soda@jollycorp.com',
                                        'amy.ge@jollycorp.com'])

    if today_week == 4:
        print('发送邮件 -- 价格带数据')
        Price_brand('home_kids_beauty@jollycorp.com')
        volume_weight(['laine@jollycorp.com', 'long.long@jollycorp.com'])

    if today_week == 5:
        print("发送邮件-婴童时尚周报")
        kids_data_goods_report('week', ['long.long@jollycorp.com',
                                        'soda@jollycorp.com',
                                        'amy.ge@jollycorp.com'])
        print("发送邮件-婴童时尚四级类目周报")
        kids_data_cate_report('week', ['long.long@jollycorp.com',
                                       'soda@jollycorp.com',
                                       'amy.ge@jollycorp.com'])
        # print('发送邮件 -- 母婴童KA商家数据')
        # KA_data(['long.long@jollycorp.com', 'camille@jollycorp.com'])

    if today_week == 6:
        print("发送邮件-类目商品动销率")
        goods_sale_rate_week_email(['long.long@jollycorp.com',
                                    'business-4th@jollycorp.com'])
        print("发送邮件-用户复购率--一级类目")
        user_resale_week_report(['long.long@jollycorp.com',
                                 'business-4th@jollycorp.com'], 1)
        print("发送邮件-用户复购率--二级类目")
        user_resale_week_report(['long.long@jollycorp.com',
                                 'business-4th@jollycorp.com'], 2)
        print("发送邮件-商品库存周报")
        goods_stock_week_report(['long.long@jollycorp.com',
                                 'business-4th@jollycorp.com'])
        print("发送邮件-新供应商周报")
        new_supp_week_report(['long.long@jollycorp.com', 'business-4th@jollycorp.com'])
        print("发送邮件-供应商周报")
        this_year_first_day_str = datetime.datetime.strftime(this_year_first_day, "%Y%m%d")
        yesterday_str = datetime.datetime.strftime(yesterday, "%Y%m%d")
        supp_week_report(this_year_first_day_str, yesterday_str, ['long.long@jollycorp.com',
                                                                  'business-4th@jollycorp.com'])


if __name__ == '__main__':
    if today_hour == 10 and today_minunts == 10:
        cate_level1_day_report(user_lst=['long.long@jollycorp.com',
                                         'business-4th@jollycorp.com'], category='家居')
        cate_level1_day_report(user_lst=['long.long@jollycorp.com',
                                         'james.hong@jollycorp.com',
                                         'rita.liang@jollycorp.com',
                                         'ailsa@jollycorp.com',
                                         'laine@jollycorp.com'], category='beauty')
        cate_level1_day_report(user_lst=['long.long@jollycorp.com',
                                         "business-5th@jollycorp.com",
                                         "allen.shen@jollycorp.com"],
                               category='孕婴童用品')
        cate_level1_day_report(user_lst=['long.long@jollycorp.com',
                                         "business-5th@jollycorp.com",
                                         "allen.shen@jollycorp.com"],
                               category='婴童时尚')

        create_goods(['long.long@jollycorp.com',
                      'tracy@jollycorp.com',
                      'yvonne.luo@jollycorp.com',
                      "nova.li@jollycorp.com",
                      'soda@jollycorp.com',
                      "allen.shen@jollycorp.com"])

        # quarter_goods_day_report(['long.long@jollycorp.com',
        #                           'richard.chen@jollycorp.com',
        #                           'tracy@jollycorp.com',
        #                           'mango@jollycorp.com',
        #                           'soda@jollycorp.com',
        #                           "allen.shen@jollycorp.com"])

        depot_stock_goods_report(['home_kids_beauty@jollycorp.com'])

        get_new_goods_sale_data(['long.long@jollycorp.com',
                                 'soda@jollycorp.com',
                                 "allen.shen@jollycorp.com",
                                 "rita.liang@jollycorp.com",
                                 "ketty@jollycorp.com"])

        send_supp_off_goods_data(['long.long@jollycorp.com',
                                  'michelle.xue@jollycorp.com',
                                  'ketty@jollycorp.com',
                                  'soda@jollycorp.com',
                                  "allen.shen@jollycorp.com"])

        sa_depot_goods_sale_num_send('家居', ['long.long@jollycorp.com',
                                            'lycoris@jollycorp.com',
                                            'ketty@jollycorp.com'])
        GMV_department_6_gmv_tar_rate_end(['long.long@jollycorp.com',
                                           'riddle@jollycorp.com',
                                           'kenny.liang@jollycorp.com'])

        get_gmv_day_report_data(['long.long@jollycorp.com', "mango@jollycorp.com", "amy.ge@jollycorp.com"])
        get_onsale_goods_data(['long.long@jollycorp.com'])
        get_new_goods_data(['long.long@jollycorp.com'])

        if today_week == 1:
            goods_view_send(['long.long@jollycorp.com',
                             'business-6th@jollycorp.com'])

            cate_level1_month_report(['long.long@jollycorp.com',
                                      "business-4th@jollycorp.com"])
            print("发送邮件-婴童时尚月报")
            kids_data_goods_report('month', ['long.long@jollycorp.com',
                                             'kidsfashion@jollycorp.com',
                                             'soda@jollycorp.com',
                                             'amy.ge@jollycorp.com',
                                             "allen.shen@jollycorp.com"])
            print("发送邮件-婴童时尚四级类目月报")
            kids_data_cate_report('month', ['long.long@jollycorp.com',
                                            'kidsfashion@jollycorp.com',
                                            'soda@jollycorp.com',
                                            'amy.ge@jollycorp.com',
                                            "allen.shen@jollycorp.com"])
            print("发送邮件-闪购数据")
            flash_week_home_send(['long.long@jollycorp.com',
                                  'lycoris@jollycorp.com',
                                  'zoe.qu@jollycorp.com'])
            flash_week_kids_send(['long.long@jollycorp.com',
                                  'rain.feng@jollycorp.com'])
            flash_week_beauty_send(['long.long@jollycorp.com',
                                    'greta@jollycorp.com',
                                    'rain.feng@jollycorp.com'])
        if today_week == 4:
            print('发送邮件 -- 价格带数据')
            Price_brand('home_kids_beauty@jollycorp.com')
            volume_weight(['laine@jollycorp.com', 'long.long@jollycorp.com'])

        if today_week == 5:
            print("发送邮件-婴童时尚周报")
            kids_data_goods_report('week', ['long.long@jollycorp.com',
                                            'soda@jollycorp.com',
                                            'amy.ge@jollycorp.com',
                                            "allen.shen@jollycorp.com"])
            print("发送邮件-婴童时尚四级类目周报")
            kids_data_cate_report('week', ['long.long@jollycorp.com',
                                           'soda@jollycorp.com',
                                           'amy.ge@jollycorp.com',
                                           "allen.shen@jollycorp.com"])

        if today_week == 6:
            print("发送邮件-类目商品动销率")
            goods_sale_rate_week_email(['long.long@jollycorp.com',
                                        'business-4th@jollycorp.com'])
            print("发送邮件-用户复购率--一级类目")
            user_resale_week_report(['long.long@jollycorp.com',
                                     'business-4th@jollycorp.com'], 1)
            print("发送邮件-用户复购率--二级类目")
            user_resale_week_report(['long.long@jollycorp.com',
                                     'business-4th@jollycorp.com'], 2)
            print("发送邮件-商品库存周报")
            goods_stock_week_report(['long.long@jollycorp.com',
                                     'business-4th@jollycorp.com'])
            print("发送邮件-新供应商周报")
            new_supp_week_report(['long.long@jollycorp.com', 'business-4th@jollycorp.com'])
            print("发送邮件-供应商周报")
            this_year_first_day_str = datetime.datetime.strftime(this_year_first_day, "%Y%m%d")
            yesterday_str = datetime.datetime.strftime(yesterday, "%Y%m%d")
            supp_week_report(this_year_first_day_str, yesterday_str, ['long.long@jollycorp.com',
                                                                      'business-4th@jollycorp.com'])

    if today_hour == 10 and today_minunts == 10:
        pass
    remove_file(base_file_path, 15, '.xlsx')
    print('完成')
