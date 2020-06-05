#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/7/30 16:37
# @Author  : Long
# @Site    :
# @File    : data_conversion.py
# @Software: PyCharm

import datetime
from src import sql_file
from src.SQL_base import run_sql
import pandas as pd
import os

today = datetime.datetime.today()
# today = datetime.datetime.strptime('2019-10-01', '%Y-%m-%d')
month0 = today + datetime.timedelta(days=-1)
month_first_day = month0.replace(day=1)
int_month_first_day = int(datetime.datetime.strftime(month_first_day, "%Y%m%d"))

months = month0.month
this_qurter = 0
if 1 <= month0.month <= 3:
    this_qurter = 1
elif 4 <= month0.month <= 6:
    this_qurter = 2
elif 7 <= month0.month <= 9:
    this_qurter = 3
else:
    this_qurter = 4


def data_to_excel(filename: str, data_list: list):
    """
    数据写入excel
    :param filename: 要写入文件的名称, 不加尾缀
    :param sheet_name_list: sheet页名称, sheet_name_list数量要与data_list元素数量对应
    :param data_list: 每个list中的数据写入到一个sheet中, 如果需要将每个df进行命名则使用tupe,
                      df_name: df; 如果不需要命名df则使用list
                      不同的sheet放到不同的dict中
                      [{sheet_name1: [(df1, df_name1), (df2, df_name2)], ...},
                       {sheet_name2: [df1, df2,...]}]
    :return: file_path
    """
    father_dirname = os.path.dirname(os.path.dirname(__file__)) + '/'
    str_today = datetime.datetime.strftime(today, '%Y%m%d')
    filename_lst = [filename + '_', str_today, '.xlsx']
    file_name = ''.join(filename_lst)
    file_path = os.path.join(father_dirname, 'excel_file/', file_name)

    excel_writer = pd.ExcelWriter(file_path, engine='xlsxwriter')
    # print(data_list)
    for item in data_list:
        for key in item:
            row_num = 0
            for value in item[key]:
                if type(value) == tuple:
                    rows = len(value[0]) + 2
                    value[0].to_excel(excel_writer, key, startrow=row_num + 2, index_label=value[1])

                    workbook = excel_writer.book
                    worksheet = excel_writer.sheets[key]
                    header_format = workbook.add_format({
                        'bold': True,
                        'text_wrap': True,
                        'valign': 'top',
                        'fg_color': '#D7E4BC',
                        'border': 1})
                    for col_num, value0 in enumerate(value[0].columns.values):
                        worksheet.write(row_num+2, col_num+1, value0, header_format)

                    row_num += rows
                else:
                    rows = len(item[key]) + 2
                    value.to_excel(excel_writer, key, startrow=row_num + 2)

                    workbook = excel_writer.book
                    worksheet = excel_writer.sheets[key]
                    header_format = workbook.add_format({
                        'bold': True,
                        'text_wrap': True,
                        'valign': 'top',
                        'fg_color': '#D7E4BC',
                        'border': 1})
                    for col_num, value0 in enumerate(value.columns.values):
                        worksheet.write(row_num+2, col_num+1, value0, header_format)

                    row_num += rows
    excel_writer.save()
    return file_path


if __name__ == '__main__':
    # employee_df, revenue, pro_income, new_up_num, rate, data0 = daily_goods_pop_conversion(is_pop_sql=True,
    #                                                                                        bu_type='home')
    # datas = [{'日报': [(employee_df, '负责人'), (revenue, '收入'),
    #                  (pro_income, '净利润'), (new_up_num, '上新数'), (rate, '毛利率')]},
    #          {'基础数据': [data0]}]
    # data_to_excel(filename='家居含POP日报', data_list=datas)
    #
    # employee_df, revenue, pro_income, new_up_num, rate, data0 = daily_goods_pop_conversion(is_pop_sql=False,
    #                                                                                        bu_type='home')
    # datas2 = [{'日报': [(employee_df, '负责人'), (revenue, '收入'),
    #                   (pro_income, '净利润'), (new_up_num, '上新数'), (rate, '毛利率')]},
    #           {'基础数据': [data0]}]
    # data_to_excel(filename='家居日报', data_list=datas2)

    ######################################################################################################
    revenue, pro_income, new_up_num, rate, data0 = daily_goods_pop_conversion(is_pop_sql=True,
                                                                                           bu_type='baby')
    datas = [{'日报': [(revenue, '收入'),
                     (pro_income, '净利润'), (new_up_num, '上新数'), (rate, '毛利率')]},
             {'基础数据': [data0]}]
    data_to_excel(filename='母婴童含POP日报', data_list=datas)

    revenue, pro_income, new_up_num, rate, data0 = daily_goods_pop_conversion(is_pop_sql=False,
                                                                                           bu_type='baby')
    datas2 = [{'日报': [(revenue, '收入'),
                      (pro_income, '净利润'), (new_up_num, '上新数'), (rate, '毛利率')]},
              {'基础数据': [data0]}]
    data_to_excel(filename='母婴童日报', data_list=datas2)
