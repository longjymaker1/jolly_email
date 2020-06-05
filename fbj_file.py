#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/12/16 16:37
# @Author  : Long
# @Site    : 
# @File    : fbj_file.py
# @Software: PyCharm
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
                   user='long',
                   password='long258')
    cur = conn.cursor(user='long')
    return cur


cur = connect_impala()

sql0 = """
insert overwrite table zybiro.bi_chili_fbj_goods_qd_all partition (ds)
select 
regexp_replace(to_date(date_sub(now(),1)),'-','') data_date,
a.goods,
a.sku,
b.goods_name,
c.sku_value,
bu,
b.category_group,
b.cate_level1_name,
b.cate_level2_name,
b.cate_level3_name,
case when h.status=1 then '下架' when h.status=0 then '在架' end is_on_sale,
c.in_price,
h.prop_price,
case when h.promote_price=0 then h.prop_price else h.promote_price end promote_price,
case when i.currency is null then '' else i.currency end currency,
case when i.bi_price_sa is null then 0 else i.bi_price_sa end bi_price_sa,
promote_price2,
nvl(free_stock,0) free_stock,
nvl(free_stock_sa,0) free_stock_sa,
nvl(free_stock_uae,0) free_stock_uae,
nvl(change_num_m,0) change_num_m,
nvl(change_num_7,0) change_num_7,
nvl(change_num_1,0) change_num_1,

nvl(goods_num_1_all,0) goods_num_1_all,
nvl(goods_num_1,0) goods_num_1,
nvl(goods_num_1_sa,0) goods_num_1_sa,
nvl(goods_num_1_uae,0) goods_num_1_uae,
nvl(goods_revenue_1,0) goods_revenue_1,
case when (goods_revenue_1=0 or goods_revenue_1 is null ) then 0 else (goods_revenue_1-goods_chengben_usd_1)/goods_revenue_1 end rate_1,

nvl(goods_num_7_all,0) goods_num_7_all,
nvl(goods_num_7,0) goods_num_7,
nvl(goods_num_7_sa,0) goods_num_7_sa,
nvl(goods_num_7_uae,0) goods_num_7_uae,
nvl(goods_revenue_7,0) goods_revenue_7,
case when (goods_revenue_7=0 or goods_revenue_7 is null ) then 0 else (goods_revenue_7-goods_chengben_usd_7)/goods_revenue_7 end rate_7,

nvl(goods_num_m_all,0) goods_num_m_all,
nvl(goods_num_m,0) goods_num_m,
nvl(goods_num_m_sa,0) goods_num_m_sa,
nvl(goods_num_m_uae,0) goods_num_m_uae,
nvl(goods_revenue_m,0) goods_revenue_m,
case when (goods_revenue_m=0 or goods_revenue_m is null ) then 0 else (goods_revenue_m-goods_chengben_usd_m)/goods_revenue_m end rate_m,
regexp_replace(to_date(date_sub(now(),1)),'-','') ds

from zybiro.zwt_1 a -- 后端sku
left join dim.dim_goods b
on a.goods=cast(b.goods_id as string)
left join jolly.who_inventory_sku_relation c
on a.goods=cast(c.goods_id as string)
and a.sku=cast(c.inventory_sku_id as string) 
left join (
-- 库存
select 
d.goods_id,d.sku_id,
sum(total_stock_num-total_order_lock_num-total_allocate_lock_num-total_return_lock_num) free_stock,
sum(case when d.depot_id=16 then (total_stock_num-total_order_lock_num-total_allocate_lock_num-total_return_lock_num) else 0 end) free_stock_sa,
sum(case when d.depot_id=15 then (total_stock_num-total_order_lock_num-total_allocate_lock_num-total_return_lock_num) else 0 end) free_stock_uae
from ods.ods_who_wms_goods_stock_total_detail d -- 库存
where  d.data_date=regexp_replace(to_date(date_sub(now(),1)),'-','')
and d.depot_id in (15,16)
and d.stock_type=1
group by d.goods_id,d.sku_id
)d
on a.goods=cast(d.goods_id as string)
and a.sku=cast(d.sku_id as string)
left join (
-- 入库量
select 
a.goods_id,a.sku_id,
sum(case when data_date>=concat(regexp_replace(substr(to_date(date_sub(now(),1)),1,7),'-',''),'01') then a.change_num else 0 end) change_num_m,
sum(case when data_date>=regexp_replace(to_date(date_sub(now(),7)),'-','') then a.change_num else 0 end) change_num_7,
sum(case when data_date=regexp_replace(to_date(date_sub(now(),1)),'-','') then a.change_num else 0 end) change_num_1
from ods.ods_who_wms_goods_stock_detail_log  a
where a.change_type in (1,2,3,4,9,11,14,15,16,18,23) 
and data_date>=regexp_replace(to_date(date_sub(now(),32)),'-','')
and a.depot_id in (15,16)
group by a.goods_id,a.sku_id
)f
on a.goods=cast(f.goods_id as string)
and a.sku=cast(f.sku_id as string)
left join (
-- 销量
select 
goods_id,
inventory_sku_id,
sum(case when pay_data>=concat(regexp_replace(substr(to_date(date_sub(now(),1)),1,7),'-',''),'01')  then a.goods_num else 0 end) goods_num_m_all,
sum(case when (pay_data>=concat(regexp_replace(substr(to_date(date_sub(now(),1)),1,7),'-',''),'01') and depod_id in (15,16)) then a.goods_num else 0 end) goods_num_m,
sum(case when (pay_data>=concat(regexp_replace(substr(to_date(date_sub(now(),1)),1,7),'-',''),'01') and depod_id=16 ) then a.goods_num else 0 end) goods_num_m_sa,
sum(case when (pay_data>=concat(regexp_replace(substr(to_date(date_sub(now(),1)),1,7),'-',''),'01') and depod_id=15 ) then a.goods_num else 0 end) goods_num_m_uae,
sum(case when pay_data>=regexp_replace(to_date(date_sub(now(),7)),'-','')  then a.goods_num else 0 end) goods_num_7_all,
sum(case when (pay_data>=regexp_replace(to_date(date_sub(now(),7)),'-','') and depod_id in (15,16)) then a.goods_num else 0 end) goods_num_7,
sum(case when (pay_data>=regexp_replace(to_date(date_sub(now(),7)),'-','') and depod_id=16) then a.goods_num else 0 end) goods_num_7_sa,
sum(case when (pay_data>=regexp_replace(to_date(date_sub(now(),7)),'-','') and depod_id=15) then a.goods_num else 0 end) goods_num_7_uae,
sum(case when pay_data=regexp_replace(to_date(date_sub(now(),1)),'-','')  then a.goods_num else 0 end) goods_num_1_all,
sum(case when (pay_data=regexp_replace(to_date(date_sub(now(),1)),'-','') and depod_id in (15,16)) then a.goods_num else 0 end) goods_num_1,
sum(case when (pay_data=regexp_replace(to_date(date_sub(now(),1)),'-','') and depod_id=16) then a.goods_num else 0 end) goods_num_1_sa,
sum(case when (pay_data=regexp_replace(to_date(date_sub(now(),1)),'-','') and depod_id=15) then a.goods_num else 0 end) goods_num_1_uae,

sum(case when (pay_data>=concat(regexp_replace(substr(to_date(date_sub(now(),1)),1,7),'-',''),'01') and depod_id in (15,16)) then a.goods_revenue else 0 end) goods_revenue_m,
sum(case when (pay_data>=regexp_replace(to_date(date_sub(now(),7)),'-','') and depod_id in (15,16)) then a.goods_revenue else 0 end) goods_revenue_7,
sum(case when (pay_data=regexp_replace(to_date(date_sub(now(),1)),'-','') and depod_id in (15,16)) then a.goods_revenue else 0 end) goods_revenue_1,

sum(case when (pay_data>=concat(regexp_replace(substr(to_date(date_sub(now(),1)),1,7),'-',''),'01') and depod_id in (15,16)) then a.goods_chengben_usd else 0 end) goods_chengben_usd_m,
sum(case when (pay_data>=regexp_replace(to_date(date_sub(now(),7)),'-','') and depod_id in (15,16)) then a.goods_chengben_usd else 0 end) goods_chengben_usd_7,
sum(case when (pay_data=regexp_replace(to_date(date_sub(now(),1)),'-','') and depod_id in (15,16)) then a.goods_chengben_usd else 0 end) goods_chengben_usd_1

from (
select 
regexp_replace(substr(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,1,10 ),'-','') pay_data,
b.goods_id,
c.inventory_sku_id,
a.depod_id,
sum(b.original_goods_number) goods_num,
sum(b.original_goods_number*b.goods_price) goods_revenue,
sum(b.in_price * b.original_goods_number)/6.74  goods_chengben_usd
from dw.dw_order_sub_order_fact a
left join jolly.who_order_goods b
on a.order_id=b.order_id
left join jolly.who_inventory_sku_relation c
on b.sku_id=cast(c.sku_id as string)
and b.goods_id=c.goods_id
and b.supp_code=c.supp_code
where  regexp_replace(substr(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,1,10 ),'-','')>=regexp_replace(to_date(date_sub(now(),32)),'-','')
and regexp_replace(substr(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,1,10 ),'-','')<regexp_replace(to_date(date_sub(now(),0)),'-','')
and a.pay_status in(1,3)
and a.site_id in (400,700,600,900,601)
-- and a.depod_id in (15,16)
group by regexp_replace(substr(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,1,10 ),'-',''),
b.goods_id,
c.inventory_sku_id,
a.depod_id
)a
group by goods_id,inventory_sku_id
)g
on a.goods=cast(g.goods_id as string)
and a.sku=cast(g.inventory_sku_id as string)
left join jolly.who_sku_relation h
on a.goods=cast(h.goods_id as string)
and c.sku_id=h.rec_id
left join (
-- 智能定价-sa
select 
*
from (
select 
a.goods_id,
b.inventory_sku_id,
a.bi_price bi_price_sa,
a.currency,
row_number()over(partition by a.goods_id,a.sku_id order by cookie_tag asc) rownum
from  ods.ods_bi_sell_price_sku a
left join jolly.who_inventory_sku_relation b
on a.sku_id=b.sku_id
and a.goods_id=b.goods_id
where data_date=regexp_replace(to_date(date_sub(now(),1)),'-','')
and country in ('SA')
and site_id in (600)
)px
where rownum=1
)i
on a.goods=cast(i.goods_id as string)
and a.sku=cast(i.inventory_sku_id as string)
left join (
-- 保底价
select  a.goods_id,b.inventory_sku_id,
case when price1!=0 and price1 is not null then price1 else price2 end promote_price2----盈亏平衡价
from zybiro.tmp_faye_fashion_balance_price_lc30_new a 
left join jolly.who_inventory_sku_relation b
on a.rec_id=b.sku_id
and a.goods_id=b.goods_id
where data_date=regexp_replace(to_date(date_sub(now(),1)),'-','')
)j
on a.goods=cast(j.goods_id as string)
and a.sku=cast(j.inventory_sku_id as string)
where a.goods  not in ('t.goods_id')
order by goods_revenue_m desc
;

"""

print("sql0-开始操作")
cur.execute(sql0)
print("sql0-结束操作")


# 将数据保存到本地
def export():
    # 读取所有结果
    sql0 = """
    select  *     
	from  	zybiro.bi_chili_fbj_goods_qd_all
    where data_date=regexp_replace(to_date(date_sub(now(),1)),'-','')
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
    df.to_csv(r'F:\pro\JollychicEmail\excel_file\bi_chili_fbj_goods_qd_all.csv', encoding='utf_8_sig', index=0)
    print('数据1导出成功')


# 调用函数
if __name__ == "__main__":
    export()

# 发邮件
# stmp服务器设置
mail_host = 'smtp.jollycorp.com'
mail_user = 'long.long@jollycorp.com'
mail_pwd = 'o7zTbCRXEWeUhduW'

# 收件人
# 主送
mail_to = ['zoe.zhang@jollycorp.com', 'FBJ_bu@jollycorp.com', 'michelle.xue@jollycorp.com',
           'richard.chen@jollycorp.com', 'long.long@jollycorp.com', 'joyce.li@jollycorp.com',
           'stella.liu@jollycorp.com', 'ketty@jollycorp.com']
# 抄送
# mail_cc = [ ]

# 表头信息
msg = MIMEMultipart('alternative')
msg['From'] = mail_user
msg['Subject'] = 'fbj商品清单'
msg['To'] = ",".join(mail_to)
# msg['Cc'] = ",".join( mail_cc )
# msg['Bcc'] = mail_bcc

# 构造csv类型的附件
xlspart0 = MIMEApplication(open(r'F:\pro\JollychicEmail\excel_file\bi_chili_fbj_goods_qd_all.csv', 'rb').read())
xlspart0.add_header('Content-Disposition', 'attachment', filename='bi_chili_fbj_goods_qd_all.csv')
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





