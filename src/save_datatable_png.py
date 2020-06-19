import numpy as np
import pandas as pd
from impala.dbapi import connect
from datetime import datetime
from datetime import timedelta
import datetime
import matplotlib
from matplotlib import pyplot as plt
# import sql_file
from src import sql_file
import os
from src.SQL_base import run_sql, connect_impala
from pyecharts.globals import CurrentConfig, NotebookType
CurrentConfig.NOTEBOOK_TYPE = NotebookType.JUPYTER_LAB

from pandas.plotting import table
matplotlib.rcParams['font.sans-serif'] = ['SimHei'] 
matplotlib.rcParams['font.family']='sans-serif'
matplotlib.rcParams['axes.unicode_minus'] = False

PNG_DIR = r"F:\pro\JollychicEmail\excel_file\imgs"

today = datetime.datetime.now() + datetime.timedelta(days=-1)
if today.day >=15:
    harf_month_ago = datetime.date.today().replace(day=1)
else:
    harf_month_ago = today - datetime.timedelta(days=15)
str_today = datetime.datetime.strftime(today, '%Y%m%d')

today_day = today.day
today_hour = today.hour
today_minunts = today.minute
today_week = today.weekday() + 1
yesterday = today
this_month_first_day = datetime.datetime(today.year, today.month, 1)
if today.day < 15:
        this_month_first_day = yesterday + datetime.timedelta(days=-15)

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


def column_percent_format(df, col, dec):
    """
    百分比格式化
    """
    return np.around(df[col] * 100, decimals=dec).astype(str) + "%"


def column_format(df, col, dec):
    """
    数据格式化
    """
    return np.around(df[col], decimals=dec)


def totle_sum(df):
    """
    跳过非int, float类型数据计算合计
    """
    num = 0
    for i in df:
        if type(i) == int or type(i) == float:
            num += i
    return num


def save_on_sale_goods_png():
    """保存在架商品数图片"""
    datadf = run_sql(sql_file.on_sale_goods_day_sql())
    datadf['data_date'] = pd.to_datetime(datadf['data_date'], format="%Y%m%d")
    df0 = datadf[datadf['data_date']>=harf_month_ago]
    df1 = df0.pivot_table(columns=['category_group'],
                       index='data_date',
                       values='goods_num',
                       aggfunc=sum).reset_index()
    df1['合计'] = df1.apply(totle_sum, axis=1)
    
    yestoday = today - datetime.timedelta(days=1)
    yestoday_str = datetime.datetime.strftime(yestoday,'%Y-%m-%d')

    fig = plt.figure(figsize=(5, 6), dpi=300)
    ax0 = fig.add_subplot(111, frame_on=False) 
    ax0.xaxis.set_visible(False)  # hide the x axis
    ax0.yaxis.set_visible(False)  # hide the y axis
    table(ax0, df1, loc='center')  # where df is your data frame

    filename_lst = ["在架商品数" + '_', str_today, '.png']
    file_name = ''.join(filename_lst)
    file_path = os.path.join(PNG_DIR, file_name)
    plt.savefig(file_path)
    plt.cla()
    return file_path


def save_new_goods_num_png():
    """保存新上架商品数图片"""
    new_goods_df = run_sql(sql_file.new_goods_day_sql())
    new_goods_df['data_date'] = pd.to_datetime(new_goods_df['data_date'], format="%Y-%m-%d")
    new_goods_df0 = new_goods_df[new_goods_df['data_date']>=harf_month_ago]
    new_goods_df0 = new_goods_df0.pivot_table(columns=['category_group'],
                                           index='data_date',
                                           values='goods_num',
                                           aggfunc=sum).reset_index()
    new_goods_df0 = new_goods_df0.fillna(0)
    # print(new_goods_df0)
    new_goods_df0['data_date'] = new_goods_df0['data_date'].apply(lambda x:x.strftime("%Y-%m-%d"))
    new_goods_df0['合计'] = new_goods_df0.apply(totle_sum, axis=1)
    new_goods_df0.loc['合计'] = new_goods_df0.apply(totle_sum, axis=0)

    fig = plt.figure(figsize=(5, 6), dpi=300)
    ax0 = fig.add_subplot(111, frame_on=False) 
    ax0.xaxis.set_visible(False)  # hide the x axis
    ax0.yaxis.set_visible(False)  # hide the y axis
    table(ax0, new_goods_df0, loc='center')  # where df is your data frame

    filename_lst = ["上新商品数" + '_', str_today, '.png']
    file_name = ''.join(filename_lst)
    file_path = os.path.join(PNG_DIR, file_name)
    plt.savefig(file_path)
    plt.cla()
    return file_path


def get_gmv_data():
    """获取当月及同期类目GMV、净利、流量数据"""
    try:
        today0 = datetime.datetime.now()
        today_str = today0.strftime("%Y%m%d")
        excel_path = r"F:\pro\JollychicEmail\excel_file"
        excel_name = "日报-gmv-data"
        file_name = "_".join([excel_name, today_str])
        file_name = file_name + ".xlsx"
        excel_path = os.path.join(excel_path, file_name)
        print(excel_path)
        gmv_df = pd.read_excel(excel_path, header=2)
        gmv_df = gmv_df.drop('data', axis=1)
    except Exception as identifier:
        gmv_df = run_sql(sql_file.gmv_day_report_sql())
        gmv_df = gmv_df.fillna(0)
        gmv_df['data_date'] = pd.to_datetime(gmv_df['data_date'], format="%Y-%m-%d")
    return gmv_df


def save_gmv_income_png(gmv_df):
    """保存GMV、净利达成同比数据"""
    last_year = today.year -1 
    home_self_this_year_df = gmv_df[((gmv_df["bu"]=="家居")|(gmv_df["bu"]=="母婴童")|(gmv_df["bu"]=="美妆"))&
                                        (gmv_df["is_pop"]==0)&
                                        (gmv_df["years0"]==today.year)&
                                        (gmv_df["month0"]==today.month)]
    home_cate1_gmv = home_self_this_year_df[['category_group', 'cate_level1_name','gmv', '净利润']].groupby(['category_group', 'cate_level1_name']).sum().reset_index()
    home_bu_gmv = home_self_this_year_df[['category_group','gmv', '净利润']].groupby(['category_group']).sum().reset_index()
    home_bu_gmv['cate_level1_name'] = "合计"

    home_df = pd.concat([home_cate1_gmv, home_bu_gmv], axis=0, sort=True)
    home_df = home_df[['category_group', 'cate_level1_name', 'gmv', '净利润']]
    home_df.columns = ['category_group', 'cate_level1_name', '当月gmv', '当月净利润']

    home_self_last_year_df = gmv_df[((gmv_df["bu"]=="家居")|(gmv_df["bu"]=="母婴童")|(gmv_df["bu"]=="美妆"))&(gmv_df["is_pop"]==0)&
                                    (gmv_df["years0"]==last_year)&(gmv_df["month0"]==today.month)]
    home_cate1_gmv_last =home_self_last_year_df[['category_group', 'cate_level1_name','gmv', '净利润']].groupby(['category_group', 'cate_level1_name']).sum().reset_index()
    home_bu_gmv_last = home_self_last_year_df[['category_group','gmv', '净利润']].groupby(['category_group']).sum().reset_index()
    home_bu_gmv_last['cate_level1_name'] = "合计"
    home_df_last = pd.concat([home_cate1_gmv_last, home_bu_gmv_last], axis=0, sort=True)
    home_df_last = home_df_last[['category_group', 'cate_level1_name', 'gmv', '净利润']]
    home_df_last.columns = ['category_group', 'cate_level1_name', '同期gmv', '同期净利润']

    home_df = home_df.merge(home_df_last, how='left')

    home_pop_this_year_df = gmv_df[((gmv_df["bu"]=="家居")|(gmv_df["bu"]=="母婴童")|(gmv_df["bu"]=="美妆"))&(gmv_df["years0"]==today.year)&(gmv_df["month0"]==today.month)]
    home_cate1_gmv_pop = home_pop_this_year_df[['category_group', 'cate_level1_name','gmv']].groupby(['category_group', 'cate_level1_name']).sum().reset_index()
    home_bu_gmv_pop = home_pop_this_year_df[['category_group','gmv']].groupby(['category_group']).sum().reset_index()
    home_bu_gmv_pop['cate_level1_name'] = "合计"
    home_pop_df = pd.concat([home_cate1_gmv_pop, home_bu_gmv_pop], axis=0, sort=True)
    home_pop_df = home_pop_df[['category_group', 'cate_level1_name', 'gmv']]
    home_pop_df.columns = ['category_group', 'cate_level1_name', 'gmv(含POP)']
    home_df = home_df.merge(home_pop_df, how='left')
    tar_excel_path = r"C:\Users\WIN7\Desktop\下载文件\目标\2020目标-GMV-净利.xlsx"
    gmv_tar_pd = pd.read_excel(tar_excel_path,sheet_name="GMV")
    incom_tar_pd = pd.read_excel(tar_excel_path,sheet_name="净利润")
    yestoday = today - datetime.timedelta(days=1)

    if len(str(yestoday.month)) == 1:
        month_col = str(yestoday.year) + "0" + str(yestoday.month)
    else:
        month_col = str(yestoday.year) + str(yestoday.month)
    
    gmv_tar_month_pd = gmv_tar_pd[['类目组', '一级类目', int(month_col)]]
    incom_tar_month_pd = incom_tar_pd[['类目组', '一级类目', int(month_col)]]
    gmv_tar_month_pd.columns = ['category_group', 'cate_level1_name', '当月GMV目标']
    incom_tar_month_pd.columns = ['category_group', 'cate_level1_name', '当月净利目标']
    tar_df = gmv_tar_month_pd.merge(incom_tar_month_pd, how='left')
    tar_df_tot = tar_df[['category_group','当月GMV目标', '当月净利目标']].groupby('category_group').sum().reset_index()
    tar_df_tot['cate_level1_name'] = "合计"
    tar_df = pd.concat([tar_df, tar_df_tot], axis=0, sort=True)
    tar_df = tar_df[['category_group', 'cate_level1_name', '当月GMV目标', '当月净利目标']]
    home_df = home_df.merge(tar_df, how='left')

    home_df['达成(自营)'] = home_df['当月gmv']/home_df['当月GMV目标']
    home_df['达成(含POP)'] = home_df['gmv(含POP)']/home_df['当月GMV目标']
    home_df['GMV同比'] = home_df['当月gmv']/home_df['同期gmv']-1 
    home_df['净利润达成'] = home_df['当月净利润']/home_df['当月净利目标']
    home_df['净利润同比'] = home_df['当月净利润']/home_df['同期净利润']-1
    home_df['当月净利率'] = home_df['当月净利润']/home_df['当月gmv']
    home_df['同期净利率'] = home_df['同期净利润']/home_df['同期gmv']

    home_df = home_df.sort_values(by=['category_group', 'cate_level1_name'])
    home_df = home_df[['category_group', 'cate_level1_name', '当月GMV目标', '当月净利目标', '当月gmv', 'gmv(含POP)', '达成(自营)', 
                   '达成(含POP)', '同期gmv', 'GMV同比' , '当月净利润', '净利润达成', '同期净利润', '净利润同比', '当月净利率', '同期净利率']].reset_index()
    home_df = home_df[['category_group', 'cate_level1_name', '当月GMV目标', '当月净利目标', '当月gmv', 'gmv(含POP)', '达成(自营)', 
                   '达成(含POP)', '同期gmv', 'GMV同比' , '当月净利润', '净利润达成', '同期净利润', '净利润同比', '当月净利率', '同期净利率']]
    

    home_df['当月GMV目标'] = home_df.apply(column_format, axis=1, col='当月GMV目标', dec=0)
    home_df['当月净利目标'] = home_df.apply(column_format, axis=1, col='当月净利目标', dec=0)
    home_df['当月gmv'] = home_df.apply(column_format, axis=1, col='当月gmv', dec=0)
    home_df['gmv(含POP)'] = home_df.apply(column_format, axis=1, col='gmv(含POP)', dec=0)
    home_df['同期gmv'] = home_df.apply(column_format, axis=1, col='同期gmv', dec=0)
    home_df['当月净利润'] = home_df.apply(column_format, axis=1, col='当月净利润', dec=0)
    home_df['同期净利润'] = home_df.apply(column_format, axis=1, col='同期净利润', dec=0)

    home_df['达成(自营)'] = home_df.apply(column_percent_format, axis=1, col='达成(自营)', dec=1)
    home_df['达成(含POP)'] = home_df.apply(column_percent_format, axis=1, col='达成(含POP)', dec=1)
    home_df['GMV同比'] = home_df.apply(column_percent_format, axis=1, col='GMV同比', dec=1)
    home_df['净利润达成'] = home_df.apply(column_percent_format, axis=1, col='净利润达成', dec=1)
    home_df['净利润同比'] = home_df.apply(column_percent_format, axis=1, col='净利润同比', dec=1)
    home_df['当月净利率'] = home_df.apply(column_percent_format, axis=1, col='当月净利率', dec=1)
    home_df['同期净利率'] = home_df.apply(column_percent_format, axis=1, col='同期净利率', dec=1)

    fig = plt.figure(figsize=(24, 8.27), dpi=300)
    ax = fig.add_subplot(111)
    q='#4a8cd8'
    colors3 = []
    for i in home_df.columns:
        colors3.append(q)
    ax.axis('off')

    gmv_table = ax.table(cellText=home_df.values,
                        colWidths=[.1]*len(home_df.columns),
                        rowLabels=home_df.index,
                        colColours = colors3,
                        colLabels=home_df.columns,
                        cellLoc = 'center', 
                        rowLoc = 'center',
                        loc='bottom',
                        bbox=[.1, 0, 1, 1]
                        )
    gmv_table.auto_set_font_size(False)
    gmv_table.set_fontsize(10)
    gmv_table.scale(1, 1)

    filename_lst = ["达成" + '_', str_today, '.png']
    file_name = ''.join(filename_lst)
    file_path = os.path.join(PNG_DIR, file_name)
    plt.savefig(file_path)
    plt.cla()
    return file_path

def this_year_gmv_self(datadf):
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
    # 根据当前日期判断是否显示上月下半月明细
    if today.day < 15:
        last_month_gmv_day = last_month_data.pivot_table(columns=['bu'],
                                                         index=['data_date'],
                                                         values='gmv',
                                                         aggfunc=sum).reset_index()
        last_month_gmv_day['data_date'] = last_month_gmv_day['data_date'].apply(lambda x:x.strftime("%Y-%m-%d"))
        last_rows = 15 - today.day
        last_month_gmv_day = last_month_gmv_day.iloc[max(last_month_gmv_day.index)-last_rows+1:,]
        last_month_gmv_day = last_month_gmv_day[['data_date', '3C', '家居', '美妆', '母婴童', '时尚', '手机']]
        last_month_all = pd.concat([last_month_gmv_day, last_month_all], axis=0, sort=True)[
            ['data_date', '3C', '家居', '美妆', '母婴童', '时尚', '手机']]
    
    this_month_gmv = datadf[(datadf['is_pop'] == 0) & (datadf['data_date'] >= this_month_first_day) & (
            datadf['data_date'] <= today)]
    this_month_gmv_df = this_month_gmv.pivot_table(columns='bu', index='data_date', values='gmv',
                                                   aggfunc='sum').reset_index()
    this_month_gmv_df['data_date'] = this_month_gmv_df['data_date'].apply(lambda x:x.strftime("%Y-%m-%d"))
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
    
    max_row_num = 0
    max_col_num = 0
    df_row_num = len(this_year_gmv_return.index) + 1
    df_col_num = len(this_year_gmv_return.columns) + 1
    if df_row_num > max_row_num:
        max_row_num = df_row_num
    if df_col_num > max_col_num:
        max_col_num = df_col_num

    fig = plt.figure(figsize=(max_col_num*1.5, max_col_num*0.5), dpi=300)
    ax = fig.add_subplot(111)
    q='#4a8cd8'
    colors3 = []
    for i in this_year_gmv_return.columns:
        colors3.append(q)
    ax.axis('off')
    ax.set_title('GMV-自营',fontsize=12)
    gmv_table = ax.table(cellText=this_year_gmv_return.values,
                        colWidths=[.1]*len(this_year_gmv_return.columns),
                        rowLabels=this_year_gmv_return.index,
                        colColours = colors3,
                        colLabels=this_year_gmv_return.columns,
                        cellLoc = 'center', 
                        rowLoc = 'center',
                        loc='bottom',
                        bbox=[.1, 0, 1, 1],
                        )
    gmv_table.auto_set_font_size(False)
    gmv_table.set_fontsize(10)
    gmv_table.scale(1, 1)
    filename_lst = ["GMV-自营" + '_', str_today, '.png']
    file_name = ''.join(filename_lst)
    file_path = os.path.join(PNG_DIR, file_name)
    plt.savefig(file_path)
    plt.cla()
    return file_path

def this_year_gmv_pop(datadf):
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
    # 根据当前日期判断是否显示上月下半月明细
    if today.day < 15:
        last_month_gmv_day = last_month_data.pivot_table(columns=['bu'],
                                                         index=['data_date'],
                                                         values='gmv',
                                                         aggfunc=sum).reset_index()
        last_month_gmv_day['data_date'] = last_month_gmv_day['data_date'].apply(lambda x:x.strftime("%Y-%m-%d"))
        last_rows = 15 - today.day
        last_month_gmv_day = last_month_gmv_day.iloc[max(last_month_gmv_day.index)-last_rows+1:,]
        last_month_gmv_day = last_month_gmv_day[['data_date', '3C', '家居', '美妆', '母婴童', '时尚', '手机']]
        last_month_all = pd.concat([last_month_gmv_day, last_month_all], axis=0, sort=True)[
            ['data_date', '3C', '家居', '美妆', '母婴童', '时尚', '手机']]
    
    this_month_gmv = datadf[(datadf['data_date'] >= this_month_first_day) &
                            (datadf['data_date'] <= yesterday)]
    this_month_gmv_df = this_month_gmv.pivot_table(columns='bu', index='data_date', values='gmv',
                                                   aggfunc='sum').reset_index()
    this_month_gmv_df['data_date'] = this_month_gmv_df['data_date'].apply(lambda x:x.strftime("%Y-%m-%d"))
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
    
    max_row_num = 0
    max_col_num = 0
    df_row_num = len(this_year_gmv_return.index) + 1
    df_col_num = len(this_year_gmv_return.columns) + 1
    if df_row_num > max_row_num:
        max_row_num = df_row_num
    if df_col_num > max_col_num:
        max_col_num = df_col_num

    fig = plt.figure(figsize=(max_col_num*1.5, max_col_num*0.5), dpi=300)
    ax = fig.add_subplot(111)
    q='#4a8cd8'
    colors3 = []
    for i in this_year_gmv_return.columns:
        colors3.append(q)
    ax.axis('off')
    ax.set_title('GMV-含POP',fontsize=12)
    gmv_table = ax.table(cellText=this_year_gmv_return.values,
                        colWidths=[.1]*len(this_year_gmv_return.columns),
                        rowLabels=this_year_gmv_return.index,
                        colColours = colors3,
                        colLabels=this_year_gmv_return.columns,
                        cellLoc = 'center', 
                        rowLoc = 'center',
                        loc='bottom',
                        bbox=[.1, 0, 1, 1],
                        )
    gmv_table.auto_set_font_size(False)
    gmv_table.set_fontsize(10)
    gmv_table.scale(1, 1)
    filename_lst = ["GMV-含POP" + '_', str_today, '.png']
    file_name = ''.join(filename_lst)
    file_path = os.path.join(PNG_DIR, file_name)
    plt.savefig(file_path)
    plt.cla()
    return file_path

def last_year_gmv_self(datadf):
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
    # 根据当前日期判断是否显示上月下半月明细
    if today.day < 15:
        last_month_gmv_day = last_month_data.pivot_table(columns=['bu'],
                                                         index=['data_date'],
                                                         values='gmv',
                                                         aggfunc=sum).reset_index()
        last_month_gmv_day['data_date'] = last_month_gmv_day['data_date'].apply(lambda x:x.strftime("%Y-%m-%d"))
        last_rows = 15 - today.day
        last_month_gmv_day = last_month_gmv_day.iloc[max(last_month_gmv_day.index)-last_rows+1:,]
        last_month_gmv_day = last_month_gmv_day[['data_date', '3C', '家居', '美妆', '母婴童', '时尚', '手机']]
        last_month_all = pd.concat([last_month_gmv_day, last_month_all], axis=0, sort=True)[
            ['data_date', '3C', '家居', '美妆', '母婴童', '时尚', '手机']]
    
    this_month_gmv = datadf[(datadf['is_pop'] == 0) & (datadf['data_date'] >= this_month_first_day_yoy) & (
            datadf['data_date'] <= yesterday_yoy)]
    this_month_gmv_df = this_month_gmv.pivot_table(columns='bu', index='data_date', values='gmv',
                                                   aggfunc='sum').reset_index()
    this_month_gmv_df['data_date'] = this_month_gmv_df['data_date'].apply(lambda x:x.strftime("%Y-%m-%d"))
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

    max_row_num = 0
    max_col_num = 0
    df_row_num = len(last_year_gmv_return.index) + 1
    df_col_num = len(last_year_gmv_return.columns) + 1
    if df_row_num > max_row_num:
        max_row_num = df_row_num
    if df_col_num > max_col_num:
        max_col_num = df_col_num

    fig = plt.figure(figsize=(max_col_num*1.5, max_col_num*0.5), dpi=300)
    ax = fig.add_subplot(111)
    q='#4a8cd8'
    colors3 = []
    for i in last_year_gmv_return.columns:
        colors3.append(q)
    ax.axis('off')
    ax.set_title('同期GMV-自营',fontsize=12)
    gmv_table = ax.table(cellText=last_year_gmv_return.values,
                        colWidths=[.1]*len(last_year_gmv_return.columns),
                        rowLabels=last_year_gmv_return.index,
                        colColours = colors3,
                        colLabels=last_year_gmv_return.columns,
                        cellLoc = 'center', 
                        rowLoc = 'center',
                        loc='bottom',
                        bbox=[.1, 0, 1, 1],
                        )
    gmv_table.auto_set_font_size(False)
    gmv_table.set_fontsize(10)
    gmv_table.scale(1, 1)
    filename_lst = ["同期GMV-自营" + '_', str_today, '.png']
    file_name = ''.join(filename_lst)
    file_path = os.path.join(PNG_DIR, file_name)
    plt.savefig(file_path)
    plt.cla()
    return file_path

def last_year_gmv_pop(datadf):
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
    # 根据当前日期判断是否显示上月下半月明细
    if today.day < 15:
        last_month_gmv_day = last_month_data.pivot_table(columns=['bu'],
                                                         index=['data_date'],
                                                         values='gmv',
                                                         aggfunc=sum).reset_index()
        last_month_gmv_day['data_date'] = last_month_gmv_day['data_date'].apply(lambda x:x.strftime("%Y-%m-%d"))
        last_rows = 15 - today.day
        last_month_gmv_day = last_month_gmv_day.iloc[max(last_month_gmv_day.index)-last_rows+1:,]
        last_month_gmv_day = last_month_gmv_day[['data_date', '3C', '家居', '美妆', '母婴童', '时尚', '手机']]
        last_month_all = pd.concat([last_month_gmv_day, last_month_all], axis=0, sort=True)[
            ['data_date', '3C', '家居', '美妆', '母婴童', '时尚', '手机']]
    
    this_month_gmv = datadf[(datadf['data_date'] >= this_month_first_day_yoy) &
                            (datadf['data_date'] <= yesterday_yoy)]
    this_month_gmv_df = this_month_gmv.pivot_table(columns='bu', index='data_date', values='gmv',
                                                   aggfunc='sum').reset_index()
    this_month_gmv_df['data_date'] = this_month_gmv_df['data_date'].apply(lambda x:x.strftime("%Y-%m-%d"))
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

    max_row_num = 0
    max_col_num = 0
    df_row_num = len(last_year_gmv_return.index) + 1
    df_col_num = len(last_year_gmv_return.columns) + 1
    if df_row_num > max_row_num:
        max_row_num = df_row_num
    if df_col_num > max_col_num:
        max_col_num = df_col_num

    fig = plt.figure(figsize=(max_col_num*1.5, max_col_num*0.5), dpi=300)
    ax = fig.add_subplot(111)
    q='#4a8cd8'
    colors3 = []
    for i in last_year_gmv_return.columns:
        colors3.append(q)
    ax.axis('off')
    ax.set_title('同期GMV-含POP',fontsize=12)
    gmv_table = ax.table(cellText=last_year_gmv_return.values,
                        colWidths=[.1]*len(last_year_gmv_return.columns),
                        rowLabels=last_year_gmv_return.index,
                        colColours = colors3,
                        colLabels=last_year_gmv_return.columns,
                        cellLoc = 'center', 
                        rowLoc = 'center',
                        loc='bottom',
                        bbox=[.1, 0, 1, 1],
                        )
    gmv_table.auto_set_font_size(False)
    gmv_table.set_fontsize(10)
    gmv_table.scale(1, 1)
    filename_lst = ["同期GMV-含POP" + '_', str_today, '.png']
    file_name = ''.join(filename_lst)
    file_path = os.path.join(PNG_DIR, file_name)
    plt.savefig(file_path)
    plt.cla()
    return file_path

def this_year_dau(datadf):
    """
    计算当月每日dau
    """
    # if today.day < 15:
    #     this_month_first_day = yesterday + datetime.timedelta(days=-15)
    this_month = datadf[(datadf['data_date'] >= this_month_first_day) & (datadf['data_date'] <= yesterday)]
    this_month_dau = this_month.pivot_table(columns='bu', index='data_date', values='点击uv',
                                            aggfunc='sum').reset_index()
    this_month_dau['data_date'] = this_month_dau['data_date'].apply(lambda x:x.strftime("%Y-%m-%d"))
    this_month_dau = this_month_dau[['data_date', '3C', '家居', '美妆', '母婴童', '时尚', '手机']]
    this_month_dau['合计'] = this_month_dau.apply(totle_sum, axis=1)

    max_row_num = 0
    max_col_num = 0
    df_row_num = len(this_month_dau.index) + 1
    df_col_num = len(this_month_dau.columns) + 1
    if df_row_num > max_row_num:
        max_row_num = df_row_num
    if df_col_num > max_col_num:
        max_col_num = df_col_num

    fig = plt.figure(figsize=(max_col_num*1.5, max_col_num*0.5), dpi=300)
    ax = fig.add_subplot(111)
    q='#4a8cd8'
    colors3 = []
    for i in this_month_dau.columns:
        colors3.append(q)
    ax.axis('off')
    ax.set_title('DAU',fontsize=12)
    gmv_table = ax.table(cellText=this_month_dau.values,
                        colWidths=[.1]*len(this_month_dau.columns),
                        rowLabels=this_month_dau.index,
                        colColours = colors3,
                        colLabels=this_month_dau.columns,
                        cellLoc = 'center', 
                        rowLoc = 'center',
                        loc='bottom',
                        bbox=[.1, 0, 1, 1],
                        )
    gmv_table.auto_set_font_size(False)
    gmv_table.set_fontsize(10)
    gmv_table.scale(1, 1)
    filename_lst = ["DAU" + '_', str_today, '.png']
    file_name = ''.join(filename_lst)
    file_path = os.path.join(PNG_DIR, file_name)
    plt.savefig(file_path)
    plt.cla()
    return file_path

def this_year_dau_ratio(datadf):
    """计算购买转化率 购买uv/点击uv"""
    # if today.day < 15:
    #     this_month_first_day = yesterday + datetime.timedelta(days=-15)
    this_month = datadf[(datadf['data_date'] >= this_month_first_day) & (datadf['data_date'] <= yesterday)]
    this_month_dau = this_month.pivot_table(columns='bu', index='data_date', values='点击uv',
                                            aggfunc='sum').reset_index()
    this_month_dau['data_date'] = this_month_dau['data_date'].apply(lambda x:x.strftime("%Y-%m-%d"))
    this_month_dau = this_month_dau[['data_date', '3C', '家居', '美妆', '母婴童', '时尚', '手机']]
    this_month_dau['合计'] = this_month_dau.apply(totle_sum, axis=1)

    this_month_sale_uv = this_month.pivot_table(columns='bu', index='data_date', values='购买uv',
                                                aggfunc='sum').reset_index()
    this_month_sale_uv['data_date'] = this_month_sale_uv['data_date'].apply(lambda x:x.strftime("%Y-%m-%d"))
    this_month_sale_uv = this_month_sale_uv[['data_date', '3C', '家居', '美妆', '母婴童', '时尚', '手机']]
    this_month_sale_uv['合计'] = this_month_sale_uv.apply(totle_sum, axis=1)

    this_dau_ratio = this_month_sale_uv[['3C', '家居', '美妆', '母婴童', '时尚', '手机', '合计']]/\
                    this_month_dau[['3C', '家居', '美妆', '母婴童', '时尚', '手机', '合计']]
    this_dau_ratio['data_date'] = this_month_dau['data_date']
    this_dau_ratio = this_dau_ratio[['data_date', '3C', '家居', '美妆', '母婴童', '时尚', '手机', '合计']]

    for i in this_dau_ratio.columns:
        if i == 'data_date':
            pass
        else:
            this_dau_ratio[i] = this_dau_ratio.apply(column_percent_format, axis=1, col=i, dec=2)
    
    max_row_num = 0
    max_col_num = 0
    df_row_num = len(this_dau_ratio.index) + 1
    df_col_num = len(this_dau_ratio.columns) + 1
    if df_row_num > max_row_num:
        max_row_num = df_row_num
    if df_col_num > max_col_num:
        max_col_num = df_col_num
    
    fig = plt.figure(figsize=(max_col_num*1.5, max_col_num*0.5), dpi=300)
    ax = fig.add_subplot(111)
    q='#4a8cd8'
    colors3 = []
    for i in this_dau_ratio.columns:
        colors3.append(q)
    ax.axis('off')
    ax.set_title('转化率',fontsize=12)
    gmv_table = ax.table(cellText=this_dau_ratio.values,
                        colWidths=[.1]*len(this_dau_ratio.columns),
                        rowLabels=this_dau_ratio.index,
                        colColours = colors3,
                        colLabels=this_dau_ratio.columns,
                        cellLoc = 'center', 
                        rowLoc = 'center',
                        loc='bottom',
                        bbox=[.1, 0, 1, 1],
                        )
    gmv_table.auto_set_font_size(False)
    gmv_table.set_fontsize(10)
    gmv_table.scale(1, 1)
    filename_lst = ["转化率" + '_', str_today, '.png']
    file_name = ''.join(filename_lst)
    file_path = os.path.join(PNG_DIR, file_name)
    plt.savefig(file_path)
    plt.cla()
    return file_path


def this_year_margin(datadf):
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
    # 根据当前日期判断是否显示上月下半月明细
    if today.day < 15:
        last_month_gmv_day = last_month.pivot_table(columns=['bu'],
                                                    index=['data_date'],
                                                    values='gmv',
                                                    aggfunc=sum).reset_index()
        last_month_cost_day = last_month.pivot_table(columns=['bu'],
                                                     index=['data_date'],
                                                     values='成本',
                                                     aggfunc=sum).reset_index()
        last_month_gmv_day['合计'] = last_month_gmv_day.apply(totle_sum, axis=1)
        last_month_cost_day['合计'] = last_month_cost_day.apply(totle_sum, axis=1)
        last_month_gmv_day = last_month_gmv_day[['data_date', '3C', '家居', '美妆', '母婴童', '时尚', '手机', '合计']]
        last_month_cost_day = last_month_cost_day[['data_date', '3C', '家居', '美妆', '母婴童', '时尚', '手机', '合计']]
        last_month_margin_day = 1-last_month_cost_day[['3C', '家居', '美妆', '母婴童', '时尚', '手机', '合计']]/\
                                last_month_gmv_day[['3C', '家居', '美妆', '母婴童', '时尚', '手机', '合计']]
        last_month_margin_day['data_date'] = last_month_cost_day['data_date']
        last_month_margin_day['data_date'] = last_month_margin_day['data_date'].apply(lambda x:x.strftime("%Y-%m-%d"))
        last_month_margin_day = last_month_margin_day[['data_date', '3C', '家居', '美妆', '母婴童', '时尚', '手机', '合计']]
        last_rows = 15 - today.day
        last_month_margin_day = last_month_margin_day.iloc[max(last_month_margin_day.index)-last_rows+1:,]
        last_month_margin_df = pd.concat([last_month_margin_day, last_month_margin_df], axis=0, sort=True)[
            ['data_date', '3C', '家居', '美妆', '母婴童', '时尚', '手机', '合计']]

    this_month_df = datadf[(datadf['is_pop'] == 0) & (datadf['data_date'] >= this_month_first_day) & (
            datadf['data_date'] <= yesterday)]
    this_month_margin = this_month_df.pivot_table(columns='bu', index='data_date', values=['gmv', '成本'],
                                                  aggfunc='sum').reset_index()
    this_month_margin['data_date'] = this_month_margin['data_date'].apply(lambda x:x.strftime("%Y-%m-%d"))
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
    
    max_row_num = 0
    max_col_num = 0
    df_row_num = len(this_year_margin_df.index) + 1
    df_col_num = len(this_year_margin_df.columns) + 1
    if df_row_num > max_row_num:
        max_row_num = df_row_num
    if df_col_num > max_col_num:
        max_col_num = df_col_num

    fig = plt.figure(figsize=(max_col_num*1.5, max_col_num*0.5), dpi=300)
    ax = fig.add_subplot(111)
    q='#4a8cd8'
    colors3 = []
    for i in this_year_margin_df.columns:
        colors3.append(q)
    ax.axis('off')
    ax.set_title('毛利率',fontsize=12)
    gmv_table = ax.table(cellText=this_year_margin_df.values,
                        colWidths=[.1]*len(this_year_margin_df.columns),
                        rowLabels=this_year_margin_df.index,
                        colColours = colors3,
                        colLabels=this_year_margin_df.columns,
                        cellLoc = 'center', 
                        rowLoc = 'center',
                        loc='bottom',
                        bbox=[.1, 0, 1, 1],
                        )
    gmv_table.auto_set_font_size(False)
    gmv_table.set_fontsize(10)
    gmv_table.scale(1, 1)
    filename_lst = ["毛利率" + '_', str_today, '.png']
    file_name = ''.join(filename_lst)
    file_path = os.path.join(PNG_DIR, file_name)
    plt.savefig(file_path)
    plt.cla()
    return file_path

def last_year_margin(datadf):
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
    # 根据当前日期判断是否显示上月下半月明细
    if today.day < 15:
        last_month_gmv_day = last_month.pivot_table(columns=['bu'],
                                                    index=['data_date'],
                                                    values='gmv',
                                                    aggfunc=sum).reset_index()
        last_month_cost_day = last_month.pivot_table(columns=['bu'],
                                                     index=['data_date'],
                                                     values='成本',
                                                     aggfunc=sum).reset_index()
        last_month_gmv_day['合计'] = last_month_gmv_day.apply(totle_sum, axis=1)
        last_month_cost_day['合计'] = last_month_cost_day.apply(totle_sum, axis=1)
        last_month_gmv_day = last_month_gmv_day[['data_date', '3C', '家居', '美妆', '母婴童', '时尚', '手机', '合计']]
        last_month_cost_day = last_month_cost_day[['data_date', '3C', '家居', '美妆', '母婴童', '时尚', '手机', '合计']]
        last_month_margin_day = 1-last_month_cost_day[['3C', '家居', '美妆', '母婴童', '时尚', '手机', '合计']]/\
                                last_month_gmv_day[['3C', '家居', '美妆', '母婴童', '时尚', '手机', '合计']]
        last_month_margin_day['data_date'] = last_month_cost_day['data_date']
        last_month_margin_day['data_date'] = last_month_margin_day['data_date'].apply(lambda x:x.strftime("%Y-%m-%d"))
        last_month_margin_day = last_month_margin_day[['data_date', '3C', '家居', '美妆', '母婴童', '时尚', '手机', '合计']]
        last_rows = 15 - today.day
        last_month_margin_day = last_month_margin_day.iloc[max(last_month_margin_day.index)-last_rows+1:,]
        last_month_margin_df = pd.concat([last_month_margin_day, last_month_margin_df], axis=0, sort=True)[
            ['data_date', '3C', '家居', '美妆', '母婴童', '时尚', '手机', '合计']]

    this_month_df = datadf[(datadf['is_pop'] == 0) & (datadf['data_date'] >= this_month_first_day_yoy) & (
            datadf['data_date'] <= yesterday_yoy)]
    this_month_margin = this_month_df.pivot_table(columns='bu', index='data_date', values=['gmv', '成本'],
                                                  aggfunc='sum').reset_index()
    this_month_margin['data_date'] = this_month_margin['data_date'].apply(lambda x:x.strftime("%Y-%m-%d"))
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
    
    max_row_num = 0
    max_col_num = 0
    df_row_num = len(this_year_margin_df.index) + 1
    df_col_num = len(this_year_margin_df.columns) + 1
    if df_row_num > max_row_num:
        max_row_num = df_row_num
    if df_col_num > max_col_num:
        max_col_num = df_col_num

    fig = plt.figure(figsize=(max_col_num*1.5, max_col_num*0.5), dpi=300)
    ax = fig.add_subplot(111)
    q='#4a8cd8'
    colors3 = []
    for i in this_year_margin_df.columns:
        colors3.append(q)
    ax.axis('off')
    ax.set_title('同期毛利率',fontsize=12)
    gmv_table = ax.table(cellText=this_year_margin_df.values,
                        colWidths=[.1]*len(this_year_margin_df.columns),
                        rowLabels=this_year_margin_df.index,
                        colColours = colors3,
                        colLabels=this_year_margin_df.columns,
                        cellLoc = 'center', 
                        rowLoc = 'center',
                        loc='bottom',
                        bbox=[.1, 0, 1, 1],
                        )
    gmv_table.auto_set_font_size(False)
    gmv_table.set_fontsize(10)
    gmv_table.scale(1, 1)
    filename_lst = ["同期毛利率" + '_', str_today, '.png']
    file_name = ''.join(filename_lst)
    file_path = os.path.join(PNG_DIR, file_name)
    plt.savefig(file_path)
    plt.cla()
    return file_path


def png_save(df_lst, df_name_lst, png_name, row_col_lst=[1,1]):
    """dataframe保存为图片
    多个df保存时需要给出行列
    df_lst : [df1, df2, df3]
    png_name : 文件路径+文件名
    """
    max_row_num = 0
    max_col_num = 0
    for df in df_lst:
        df_row_num = len(df.index) + 1
        df_col_num = len(df.columns) + 1
        if df_row_num > max_row_num:
            max_row_num = df_row_num
        if df_col_num > max_col_num:
            max_col_num = df_col_num
    fig = plt.figure(figsize=(max_col_num*1.5, max_col_num*0.5), dpi=450)
    sub_num = str(row_col_lst[0]) + str(row_col_lst[1])
    for df in df_lst:
        add_subplot_num = sub_num + str(df_lst.index(df)+1)
        ax0 = fig.add_subplot(add_subplot_num)
        q='#4a8cd8'
        colors3 = []
        for i in df.columns:
            colors3.append(q)
        ax0.axis('off')
        ax0.set_title(df_name_lst[df_lst.index(df)],loc='left')
        img0 = ax0.table(cellText=df.values,
                        colWidths=[.1]*len(df.columns),
                        rowLabels=df.index,
                        colColours = colors3,
                        colLabels=df.columns,
                        cellLoc = 'center', 
                        rowLoc = 'center',
                        loc='bottom',
                        bbox=[.1, 0, 1, 1],
                    )

        img0.auto_set_font_size(False)
        img0.set_fontsize(12)
        img0.scale(1, 1)
    plt.savefig(png_name)
    plt.cla()


def bu_data_png_save(datadf):
    """保存各bu自营gmv、pop_gmv、毛利、流量数据图片"""
    this_year_gmv_self_df = this_year_gmv_self(datadf)
    this_year_gmv_all_df = this_year_gmv_pop(datadf)
    this_year_margin_df = this_year_margin(datadf)
    this_year_dau_df = this_year_dau(datadf)

    last_year_gmv_self_df = last_year_gmv_self(datadf)
    last_year_gmv_all_df = last_year_gmv_pop(datadf)
    last_year_margin_df = last_year_margin(datadf)

    filename_lst = ["当月GMV" + '_', str_today, '.png']
    file_name = ''.join(filename_lst)
    file_path = os.path.join(PNG_DIR, file_name)
    png_save(df_lst=[this_year_gmv_self_df, this_year_gmv_all_df, this_year_margin_df, this_year_dau_df],
             df_name_lst=['GMV-自营','GMV-含pop','毛利率','DAU'],
             png_name=file_path, 
             row_col_lst=[2,2])

    yoy_filename_lst = ["同期GMV" + '_', str_today, '.png']
    yoy_file_name = ''.join(yoy_filename_lst)
    yoy_file_path = os.path.join(PNG_DIR, yoy_file_name)
    png_save(df_lst=[last_year_gmv_self_df, last_year_gmv_all_df, last_year_margin_df], 
             df_name_lst=['同期GMV-自营','同期GMV-含pop','同期毛利率'],
             png_name=yoy_file_path, 
             row_col_lst=[2,2])
    return file_path, yoy_file_path




if __name__ == '__main__':
    print(save_on_sale_goods_png())
    