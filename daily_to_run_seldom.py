# -*- coding: utf-8 -*-
"""
Created on Wed Sep 19 21:45:47 2018

@author: WIN7
"""

from impala.dbapi import connect
from impala.util import as_pandas
from datetime import datetime
from datetime import timedelta
import pandas as pd

# 邮件相关的库
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from email.mime.application import MIMEApplication
from email.header import Header


def connect_impala():
    conn = connect(host='172.31.2.214',
                   port=25000,
                   user='elaine',
                   password='elaine')
    cur = conn.cursor(user='elaine')
    return cur


cur = connect_impala()


# 将数据保存到本地
def export0():
    # 读取所有结果
    sql0 = """
    --------------去化率
select (case when depot_id=36 then 16 else depot_id end) depot_id,business_group,category_group_1
    ,sum(free_stock_num01)free_stock_num01,sum(free_stock_num88)free_stock_num88,sum(change_num_ru)change_num_ru
    ,sum(least(free_stock_num01,change_num_chu))change_num_chu
    ,sum(free_stock_amount01)free_stock_amount01,sum(free_stock_amount88)free_stock_amount88,sum(change_amount_ru)change_amount_ru
    ,sum(least(free_stock_amount01,change_amount_chu))change_amount_chu
from(
select zx.goods_id,zx.depot_id,cc.business_group
    ,(case when g.cate_level2_name in ('Large Appliance','TVs&Acc') then '大家电' 
    when cc.category_group_1='Appliances' then '小家电'
    else cc.category_group_1 end) category_group_1
    ,a.free_stock_num free_stock_num01,b.free_stock_num free_stock_num88,c.change_num_ru,least(c.change_num_chu,a.free_stock_num)change_num_chu
    ,a.free_stock_num*d.in_price free_stock_amount01,b.free_stock_num*d.in_price free_stock_amount88
    ,c.change_num_ru*d.in_price change_amount_ru,least(c.change_num_chu,a.free_stock_num)*d.in_price change_amount_chu
from zybiro.tmp_elaine_goods_zhixiao11_lc180 zx
left join (
----期初
select sku_id,goods_id,depot_id
    ,sum(free_stock_num) free_stock_num
from ods.ods_who_wms_goods_stock_total_detail
where data_date='20200204'
and stock_type=1
group by 1,2,3) a on zx.goods_id=a.goods_id and zx.depot_id=a.depot_id
left join (
----当前库存
select sku_id,goods_id,depot_id
    ,sum(free_stock_num) free_stock_num
from ods.ods_who_wms_goods_stock_total_detail
where data_date=from_unixtime(unix_timestamp()-1*24*3600,'yyyyMMdd')
and stock_type=1
group by 1,2,3) b on zx.goods_id=b.goods_id and a.sku_id=b.sku_id and a.depot_id=b.depot_id
left join (
----期间入库
----进
select sku_id,a.goods_id,depot_id
    ,sum(case when change_type in (1,2,3,4,9,11,14,16,18,23) then change_num else 0 end)change_num_ru  ---调拨出库13 入库15 不算
    ,sum(case when change_type in (5,6,10,12,17,19,20,21,22,24,25) then change_num else 0 end)change_num_chu
from ods.ods_who_wms_goods_stock_detail_log a
where data_date>='20200205'
--and change_type in (1,2,3,4,9,11,14,16,18)
group by 1,2,3
) c on zx.goods_id=c.goods_id and a.sku_id=c.sku_id and a.depot_id=c.depot_id
left join jolly.who_inventory_sku_relation d on a.sku_id=d.inventory_sku_id
left join dim.dim_goods g on zx.goods_id=g.goods_id 
left join zybiro.bi_chili_cat_group cc on g.cate_level1_name=cc.cat_level1_name) t
group by 1,2,3
order by 1,2,3;
    """
    print('sql0-开始执行')
    cur.execute(sql0)
    print('sql0-执行完毕')
    results1 = cur.fetchall()
    # 获取数据字段名称
    fields = cur.description
    field = []
    for i in fields:
        field.append(i[0])
    # 将数据保存到本地
    df = pd.DataFrame(list(results1), columns=field)
    df.to_csv('zhixiao_out_stock.csv', encoding='utf_8_sig', index=0)
    print('数据0导出成功')


# 调用函数
if __name__ == "__main__":
    export0()

# 发邮件
# stmp服务器设置
mail_host = 'smtp.jollycorp.com'
mail_user = 'elaine.zhang@jollycorp.com'
mail_pwd = 'syy1222@JC'

# 收件人
# 主送
mail_to = ['elaine.zhang@jollycorp.com', 'enzo@jollycorp.com']
# 抄送
# mail_cc = ['faye@jollycorp.com']

# 表头信息
msg = MIMEMultipart('alternative')
msg['From'] = mail_user
msg['Subject'] = '滞销商品去化情况'
msg['To'] = ",".join(mail_to)
# msg['Cc'] = ",".join( mail_cc )
# msg['Bcc'] = mail_bcc

# 构造csv类型的附件
xlspart0 = MIMEApplication(open('zhixiao_out_stock.csv', 'rb').read())
xlspart0.add_header('Content-Disposition', 'attachment', filename='zhixiao_out_stock.csv')
msg.attach(xlspart0)  # 把附件添加到邮件中

try:
    s = smtplib.SMTP()
    s = smtplib.SMTP_SSL(mail_host, 465)
    s.connect(mail_host)
    # login
    s.login(mail_user, mail_pwd)
    # send mail
    s.sendmail(mail_user, mail_to, msg.as_string())
    s.close()
    print('success')
except Exception as e:
    print('Exception: ', e)


# 将数据保存到本地
def export0():
    # 读取所有结果
    sql0 = """
-----通拓商品蓄水池状态
--未处理	不可上传	已审核	已复审	待上传	已上传
select cc.cat_id,business_group,category_group_1,cate_level1_name,provide_code,cn goods_num,onsale_goods
    ,op_status0,op_status2,op_status1,op_status8,op_status6,op_status7
from
(select cat_id,cc.business_group,cc.category_group_1,cate_level1_name
 from dim.dim_goods g
 inner join zybiro.bi_chili_cat_group cc on g.cate_level1_name=cc.cat_level1_name
 group by 1,2,3,4) cc
left join (
select provide_code,cat_id,count(1) cn,count(case when is_on_sale=1 then 1 end) onsale_goods
from dim.dim_jc_goods g
where provide_code in ('3C8','9U0')
group by 1,2) a on cc.cat_id=a.cat_id
left join 
(
select supp_code,cat_id --,op_status --
    ,count(1) op_status
    ,count(case when op_status=0 then 1 end) op_status0
    ,count(case when op_status=2 then 1 end) op_status2
    ,count(case when op_status=1 then 1 end) op_status1
    ,count(case when op_status=8 then 1 end) op_status8
    ,count(case when op_status=6 then 1 end) op_status6
    ,count(case when op_status=7 then 1 end) op_status7
from jolly.who_product_pool
where supp_code in ('3C8','9U0')
group by 1,2) b
on a.provide_code=b.supp_code and cc.cat_id=b.cat_id
where cn>0 or op_status>0;
    """
    print('sql0-开始执行')
    cur.execute(sql0)
    print('sql0-执行完毕')
    results1 = cur.fetchall()
    # 获取数据字段名称
    fields = cur.description
    field = []
    for i in fields:
        field.append(i[0])
    # 将数据保存到本地
    df = pd.DataFrame(list(results1), columns=field)
    df.to_csv('product_pool_status_3C8.csv', encoding='utf_8_sig', index=0)
    print('数据0导出成功')


# 调用函数
if __name__ == "__main__":
    export0()

# 发邮件
# stmp服务器设置
mail_host = 'smtp.jollycorp.com'
mail_user = 'elaine.zhang@jollycorp.com'
mail_pwd = 'syy1222@JC'

# 收件人
# 主送
mail_to = ['elaine.zhang@jollycorp.com', 'meli@jollycorp.com']
# 抄送
# mail_cc = ['faye@jollycorp.com']

# 表头信息
msg = MIMEMultipart('alternative')
msg['From'] = mail_user
msg['Subject'] = '跨境大卖蓄水池状态'
msg['To'] = ",".join(mail_to)
# msg['Cc'] = ",".join( mail_cc )
# msg['Bcc'] = mail_bcc

# 构造csv类型的附件
xlspart0 = MIMEApplication(open('product_pool_status_3C8.csv', 'rb').read())
xlspart0.add_header('Content-Disposition', 'attachment', filename='product_pool_status_3C8.csv')
msg.attach(xlspart0)  # 把附件添加到邮件中

try:
    s = smtplib.SMTP()
    s = smtplib.SMTP_SSL(mail_host, 465)
    s.connect(mail_host)
    # login
    s.login(mail_user, mail_pwd)
    # send mail
    s.sendmail(mail_user, mail_to, msg.as_string())
    s.close()
    print('success')
except Exception as e:
    print('Exception: ', e)
