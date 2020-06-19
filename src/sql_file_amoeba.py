#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020/6/19 11:50
# @Author  : Long
# @Site    :
# @File    : sql_file_amoeba.py
# @Software: PyCharm

import datetime


today = datetime.datetime.today()
# today = datetime.datetime.strptime('2019-10-01', '%Y-%m-%d')
month_first_day = today.replace(day=1)
yestoday_date = today + datetime.timedelta(-1)

yestoday = datetime.datetime.strftime(today + datetime.timedelta(-1), "%Y%m%d")
month_first_day_str = datetime.datetime.strftime(month_first_day, "%Y%m%d")
if today.month == 1:
    last_month_fisrt_day = datetime.datetime.strftime(month_first_day.replace(month=12, year=month_first_day.year-1),
                                                      "%Y%m%d")
else:
    last_month_fisrt_day = datetime.datetime.strftime(month_first_day.replace(month=month_first_day.month-1), "%Y%m%d")
last_month_last_day = datetime.datetime.strftime(month_first_day + datetime.timedelta(-1), "%Y%m%d")

quarter_first_month = (yestoday_date.month - 1) - (yestoday_date.month - 1) % 3 + 1
this_quarter_start = datetime.datetime.strftime(datetime.datetime(today.year, quarter_first_month, 1), "%Y%m%d")

half_year_ago = datetime.datetime.strftime(today + datetime.timedelta(-181), "%Y%m%d")
quer_ago = datetime.datetime.strftime(today + datetime.timedelta(-91), "%Y%m%d")


class GoodsOnWeekSQL:
    """
    阿米巴在架数恢复；TOP款恢复
    """
    def __init__(self):
        pass

    def week_goods_restore_JC_Dealy_sql(self):
        """
        JC在架、JC自营在架、JC POP在架、dealy在架，1月在架是以2020年1月1号为基础
        :return: sql_str
        """
        sqlmsg = """
        select
            *
        from (
            select -- 1月在架情况
                "1月在架数" as lable
                ,count(case when a.is_jc_on_sale = 1 then a.goods_id else null end)/count(distinct a.data_date) 'JC在架'
                ,count(case when a.is_jc_on_sale = 1 and b.supplier_genre != 11 then a.goods_id else null end)/count(distinct a.data_date) 'JC自营在架'
                ,count(case when a.is_jc_on_sale = 1 and b.supplier_genre = 11 then a.goods_id else null end)/count(distinct a.data_date) 'JC_POP在架'
                ,count(case when a.is_dealy_on_sale = 1 then a.goods_id else null end) 'Dealy在架'
            from (
               select distinct data_date,goods_id,is_jc_on_sale,is_dealy_on_sale
               from dim.dim_goods_extend 
               where data_date >= '20200101'
               and data_date <= "20200131"
               and (is_jc_on_sale = 1 or is_dealy_on_sale = 1)
            ) a 
            left join dim.dim_goods b on a.goods_id = b.goods_id
            union all
            select  -- 各周在架情况
                a2.month_week_lable as lable
                ,count(case when is_jc_on_sale = 1 then goods_id else null end)/count(distinct data_date0)  as "JC在架"
                ,count(case when is_self = 1 and is_jc_on_sale = 1 then goods_id else null end)/count(distinct data_date0) as "JC_SELF在架"
                ,count(case when is_self = 0 and is_jc_on_sale = 1 then goods_id else null end)/count(distinct data_date0) as "JC_POP在架"
                ,count(case when is_dealy_on_sale = 1 then goods_id else null end)/count(distinct data_date0) as "Dealy在架"
            from(
                select
                    a1.goods_id
                    ,data_date0
                    ,a1.is_dealy_on_sale
                    ,a1.is_jc_on_sale
                    ,concat(cast(a1.month0 as string),"月","第", cast(a1.week_year - month_week_year + 1 as string),"周") as month_week_lable
                    ,case when dg.supplier_genre = 11 then 0 else 1 end as is_self
                from(
                    select
                        a0.data_date0
                        ,a0.month0
                        ,a0.goods_id
                        ,a0.is_jc_on_sale
                        ,a0.is_dealy_on_sale
                        ,week_year
                        ,min(a0.week_year) over(partition by a0.month0) as month_week_year
                    from(
                        select 
                            distinct data_date
                            ,from_unixtime(unix_timestamp(data_date,"yyyyMMdd"),'yyyy-MM-dd') as data_date0
                            ,month(from_unixtime(unix_timestamp(data_date,"yyyyMMdd"),'yyyy-MM-dd')) as month0
                            ,case when weekofyear(from_unixtime(unix_timestamp(data_date,"yyyyMMdd"),'yyyy-MM-dd'))=18 then 19
                                    else weekofyear(from_unixtime(unix_timestamp(data_date,"yyyyMMdd"),'yyyy-MM-dd')) 
                                end as week_year
                            ,goods_id
                            ,is_jc_on_sale
                            ,is_dealy_on_sale
                        from dim.dim_goods_extend 
                        where data_date >= '20200501'
                        and data_date <= from_timestamp(date_sub(now(),1), "yyyyMMdd")
                        and (is_jc_on_sale = 1 or is_dealy_on_sale = 1 )
                    ) as a0
                ) as a1
                left join dim.dim_goods as dg
                on a1.goods_id = dg.goods_id
            ) as a2
            group by 1
        ) as a3
        order by lable;
        """
        return sqlmsg

    def week_goods_restore_amoeba(self):
        """
        各阿米巴在架款恢复，1月在架是以2020年1月1号为基础
        :return: sql_str
        """
        sqlmsg = """
        select
            *
        from(
            select -- 1月在架情况
                "1月在架数" as lable
                ,count(case when cg.department = "一部" then a.goods_id else null end)/count(distinct a.data_date) as `一部`
                ,count(case when cg.department = "二部" then a.goods_id else null end)/count(distinct a.data_date) as `二部`
                ,count(case when cg.department = "三部" then a.goods_id else null end)/count(distinct a.data_date) as `三部`
                ,count(case when cg.department = "四部" then a.goods_id else null end)/count(distinct a.data_date) as `四部`
                ,count(case when cg.department = "五部" then a.goods_id else null end)/count(distinct a.data_date) as `五部`
                ,count(case when cg.department = "六部" then a.goods_id else null end)/count(distinct a.data_date) as `六部`
                ,count(case when cg.department = "七部" then a.goods_id else null end)/count(distinct a.data_date) as `七部`
            from (
               select dge.data_date,dge.goods_id,dge.is_jc_on_sale
               from dim.dim_goods_extend as dge
               left join dim.dim_goods as dg
               on dge.goods_id = dg.goods_id
               where data_date >= '20200101'
               and data_date <= "20200131"
               and is_jc_on_sale = 1
               and dg.supplier_genre != 11
            ) a 
            left join dim.dim_goods b on a.goods_id = b.goods_id
            left join zybiro.bi_longjy_category_group_new as cg
            on b.cate_level1_name = cg.cate_level1_name
            union all
            select  -- 各周在架情况
                a2.month_week_lable as lable
                ,count(case when a2.department = "一部" and a2.is_self = 1 then a2.goods_id else null end)/count(distinct a2.data_date0) as "一部"
                ,count(case when a2.department = "二部" and a2.is_self = 1 then a2.goods_id else null end)/count(distinct a2.data_date0) as "二部"
                ,count(case when a2.department = "三部" and a2.is_self = 1 then a2.goods_id else null end)/count(distinct a2.data_date0) as "三部"
                ,count(case when a2.department = "四部" and a2.is_self = 1 then a2.goods_id else null end)/count(distinct a2.data_date0) as "四部"
                ,count(case when a2.department = "五部" and a2.is_self = 1 then a2.goods_id else null end)/count(distinct a2.data_date0) as "五部"
                ,count(case when a2.department = "六部" and a2.is_self = 1 then a2.goods_id else null end)/count(distinct a2.data_date0) as "六部"
                ,count(case when a2.department = "七部" and a2.is_self = 1 then a2.goods_id else null end)/count(distinct a2.data_date0) as "七部"
            from(
                select
                    a1.goods_id
                    ,a1.data_date0
                    ,cg.department
                    ,a1.is_jc_on_sale
                    ,concat(cast(a1.month0 as string),"月","第", cast(a1.week_year - month_week_year + 1 as string),"周") as month_week_lable
                    ,case when dg.supplier_genre = 11 then 0 else 1 end as is_self
                from(
                    select
                        a0.data_date0
                        ,a0.month0
                        ,a0.goods_id
                        ,a0.is_jc_on_sale
                        ,week_year
                        ,min(a0.week_year) over(partition by a0.month0) as month_week_year
                    from(
                        select 
                            distinct data_date
                            ,from_unixtime(unix_timestamp(data_date,"yyyyMMdd"),'yyyy-MM-dd') as data_date0
                            ,month(from_unixtime(unix_timestamp(data_date,"yyyyMMdd"),'yyyy-MM-dd')) as month0
                            ,case when weekofyear(from_unixtime(unix_timestamp(data_date,"yyyyMMdd"),'yyyy-MM-dd'))=18 then 19
                                    else weekofyear(from_unixtime(unix_timestamp(data_date,"yyyyMMdd"),'yyyy-MM-dd')) 
                                end as week_year
                            ,goods_id
                            ,is_jc_on_sale
                        from dim.dim_goods_extend 
                        where data_date >= '20200501'
                        and data_date <= from_timestamp(date_sub(now(),1), "yyyyMMdd")
                        and is_jc_on_sale = 1
                    ) as a0
                ) as a1
                left join dim.dim_goods as dg
                on a1.goods_id = dg.goods_id
                left join zybiro.bi_longjy_category_group_new as cg
                on dg.cate_level1_name = cg.cate_level1_name
            ) as a2
            group by 1
        ) as a3
        order by lable
        """
        return sqlmsg

    def week_top_goods_restore_amoeba(self):
        """
        自营各阿米巴TOP款恢复
        :return:sql_str
        """
        sqlmsg = """
        with month_jc_self as (
            select
                a2.department
                ,a2.category_group
                ,a2.cate_level1_name
                ,a2.cate_level2_name
                ,a2.goods_id
                ,a2.revenue
                ,a2.rn
            from (
                select
                    a1.department
                    ,a1.category_group
                    ,a1.cate_level1_name
                    ,a1.cate_level2_name
                    ,a1.goods_id
                    ,a1.revenue
                    ,a1.rn
                from(
                    select
                        a0.department
                        ,a0.category_group
                        ,a0.cate_level1_name
                        ,a0.cate_level2_name
                        ,a0.goods_id
                        ,a0.revenue
                        ,a0.cat1_gmv_sum
                        ,a0.rn
                        ,sum(a0.revenue) over(partition by a0.cate_level2_name order by a0.cate_level2_name,rn) as rn_sum
                    from(
                        select
                            gs.department
                            ,gs.category_group
                            ,gs.cate_level1_name
                            ,gs.cate_level2_name
                            ,gs.goods_id
                            ,gs.revenue
                            ,sum(gs.revenue) over(partition by gs.cate_level2_name) as cat1_gmv_sum
                            ,row_number() over(partition by gs.cate_level2_name order by gs.revenue desc) as rn
                        from (  
                            select 
                                cg.department
                                ,cat.category_group
                                ,case when c.cate_level1_name="Beauty" then c.cate_level2_name else c.cate_level1_name end as cate_level1_name
                                ,c.cate_level2_name
                                ,b.goods_id
                                ,sum(case when cg.department in ('一部','二部','三部') and ((a.tru_pay_time>= '2019-06-01' and a.tru_pay_time < '2019-09-01' ) or (a.tru_pay_time>= '2020-04-01' and a.tru_pay_time < '2019-06-01' )) then b.original_goods_number*b.goods_price
                                            when cg.department in ('五部') and c.cate_level1_name = "Kid's Clothing" and ((a.tru_pay_time>= '2019-06-01' and a.tru_pay_time < '2019-09-01' ) or (a.tru_pay_time>= '2020-04-01' and a.tru_pay_time < '2019-06-01' )) then b.original_goods_number*b.goods_price
                                        else b.original_goods_number*b.goods_price end) revenue
                            from dw.dw_order_goods_fact b 
                            inner join (select a.*,case when a.pay_id=41 then a.pay_time else a.result_pay_time end as tru_pay_time from dw.dw_order_fact a ) a on a.order_id=b.order_id
                            left join dim.dim_goods c on c.goods_id=b.goods_id
                            left join jolly.who_esoloo_supplier  e on  c.provider_code = e.code
                            left join dim.dim_goods_category_group_new as cat
                            on c.cate_level1_name = cat.cate_level1_name
                            left join zybiro.bi_longjy_category_group_new as cg
                            on c.cate_level1_name = cg.cate_level1_name
                            where ((a.tru_pay_time>= '2020-01-01' and a.tru_pay_time < '2020-06-01') or (a.tru_pay_time>= '2019-06-01' and a.tru_pay_time < '2019-09-01'))
                            and a.site_id  in(400,600,700,601,900) 
                            and a.pay_status in(1,3)
                            and c.supplier_genre != 11
                            and cg.department != "七部"
                            group by 1,2,3,4,5
                        ) as gs
                    ) as a0
                ) as a1
                where a1.rn_sum/a1.cat1_gmv_sum <= 0.5
                union all
                select
                    a1.department
                    ,a1.category_group
                    ,a1.cate_level1_name
                    ,a1.cate_level2_name
                    ,a1.goods_id
                    ,a1.revenue
                    ,a1.rn
                from(
                    select
                        a0.department
                        ,a0.category_group
                        ,a0.cate_level1_name
                        ,a0.cate_level2_name
                        ,a0.goods_id
                        ,a0.revenue
                        ,a0.cat1_gmv_sum
                        ,a0.rn
                        ,sum(a0.revenue) over(partition by a0.category_group order by a0.category_group,rn) as rn_sum
                    from(
                        select
                            gs.department
                            ,gs.category_group
                            ,gs.cate_level1_name
                            ,gs.cate_level2_name
                            ,gs.goods_id
                            ,gs.revenue
                            ,sum(gs.revenue) over(partition by gs.category_group) as cat1_gmv_sum
                            ,row_number() over(partition by gs.category_group order by gs.revenue desc) as rn
                        from (  
                            select 
                                cg.department
                                ,cat.category_group
                                ,c.cate_level1_name
                                ,c.cate_level2_name
                                ,b.goods_id
                                ,sum(b.original_goods_number*b.goods_price) revenue
                            from dw.dw_order_goods_fact b 
                            inner join (select a.*,case when a.pay_id=41 then a.pay_time else a.result_pay_time end as tru_pay_time from dw.dw_order_fact a ) a on a.order_id=b.order_id
                            left join dim.dim_goods c on c.goods_id=b.goods_id
                            left join jolly.who_esoloo_supplier  e on  c.provider_code = e.code
                            left join dim.dim_goods_category_group_new as cat
                            on c.cate_level1_name = cat.cate_level1_name
                            left join zybiro.bi_longjy_category_group_new as cg
                            on c.cate_level1_name = cg.cate_level1_name
                            where a.tru_pay_time>= '2020-04-01' and a.tru_pay_time < from_timestamp(date_sub(now(),1), "yyyyMMdd")
                            and a.site_id  in(400,600,700,601,900) 
                            and a.pay_status in(1,3)
                            and c.supplier_genre != 11
                            and cg.department = "七部"
                            group by 1,2,3,4,5
                        ) as gs
                    ) as a0
                ) as a1
                where a1.rn_sum/a1.cat1_gmv_sum <= 0.5
            ) a2 
        )
        
        
        select
            *
        from(
            select
                "0TOP款数量" as month_week_lable
                ,count(distinct case when a.department = "一部" then a.goods_id else null end) as `一部`
                ,count(distinct case when a.department = "二部" then a.goods_id else null end) as `二部`
                ,count(distinct case when a.department = "三部" then a.goods_id else null end) as `三部`
                ,count(distinct case when a.department = "四部" then a.goods_id else null end) as `四部`
                ,count(distinct case when a.department = "五部" then a.goods_id else null end) as `五部`
                ,count(distinct case when a.department = "六部" then a.goods_id else null end) as `六部`
                ,count(distinct case when a.department = "七部" then a.goods_id else null end) as `七部`
            from month_jc_self a --TOP款数
            union all
            select
                a0.month_week_lable
                ,count(distinct case when a0.department = "一部" then a0.goods_base else null end) as `一部`
                ,count(distinct case when a0.department = "二部" then a0.goods_base else null end) as `二部`
                ,count(distinct case when a0.department = "三部" then a0.goods_base else null end) as `三部`
                ,count(distinct case when a0.department = "四部" then a0.goods_base else null end) as `四部`
                ,count(distinct case when a0.department = "五部" then a0.goods_base else null end) as `五部`
                ,count(distinct case when a0.department = "六部" then a0.goods_base else null end) as `六部`
                ,count(distinct case when a0.department = "七部" then a0.goods_base else null end) as `七部`
            from(
                select
                    mjs.department
                    ,mjs.goods_id as goods_base
                    ,gw.goods_id as goods_week
                    ,gw.data_date0
                    ,gw.month0
                    ,gw.week_year
                    ,min(gw.week_year) over(partition by gw.month0) as month_week_year
                    ,concat(cast(gw.month0 as string),"月","第", cast(gw.week_year - min(gw.week_year) over(partition by gw.month0) + 1 as string),"周") as month_week_lable
                from month_jc_self as mjs
                left join (
                    select
                        dg.goods_id
                        ,from_unixtime(unix_timestamp(data_date,"yyyyMMdd"),'yyyy-MM-dd') as data_date0
                        ,month(from_unixtime(unix_timestamp(data_date,"yyyyMMdd"),'yyyy-MM-dd')) as month0
                        ,case when weekofyear(from_unixtime(unix_timestamp(data_date,"yyyyMMdd"),'yyyy-MM-dd'))=18 then 19
                                else weekofyear(from_unixtime(unix_timestamp(data_date,"yyyyMMdd"),'yyyy-MM-dd')) 
                            end as week_year
                        ,count(dg.goods_id) as goods_num
                    from dim.dim_goods as dg
                    left join dim.dim_goods_extend as dge
                    on dg.goods_id = dge.goods_id
                    left join zybiro.bi_longjy_category_group_new as cg
                    on cg.cate_level1_name = cg.cate_level1_name
                    where dge.data_date >= '20200501'
                    and dge.data_date <= from_timestamp(date_sub(now(),1), "yyyyMMdd")
                    and dge.is_jc_on_sale = 1
                    and dg.supplier_genre != 11
                    group by 1,2,3,4
                ) as gw
                on mjs.goods_id = gw.goods_id
            ) as a0
            where a0.month_week_lable is not null 
            group by 1
        ) as a1
        order by a1.month_week_lable
        """
        return sqlmsg

    def week_top_goods_restore_class2(self):
        """
        自营各阿米巴二级类目TOP款恢复
        :return:sql_str
        """
        sqlmsg = """
        with month_jc_self as (
            select
                a2.department
                ,a2.category_group
                ,a2.cate_level1_name
                ,a2.cate_level2_name
                ,a2.goods_id
                ,a2.revenue
                ,a2.rn
            from (
                select
                    a1.department
                    ,a1.category_group
                    ,a1.cate_level1_name
                    ,a1.cate_level2_name
                    ,a1.goods_id
                    ,a1.revenue
                    ,a1.rn
                from(
                    select
                        a0.department
                        ,a0.category_group
                        ,a0.cate_level1_name
                        ,a0.cate_level2_name
                        ,a0.goods_id
                        ,a0.revenue
                        ,a0.cat1_gmv_sum
                        ,a0.rn
                        ,sum(a0.revenue) over(partition by a0.cate_level2_name order by a0.cate_level2_name,rn) as rn_sum
                    from(
                        select
                            gs.department
                            ,gs.category_group
                            ,gs.cate_level1_name
                            ,gs.cate_level2_name
                            ,gs.goods_id
                            ,gs.revenue
                            ,sum(gs.revenue) over(partition by gs.cate_level2_name) as cat1_gmv_sum
                            ,row_number() over(partition by gs.cate_level2_name order by gs.revenue desc) as rn
                        from (  
                            select 
                                cg.department
                                ,cat.category_group
                                ,case when c.cate_level1_name="Beauty" then c.cate_level2_name else c.cate_level1_name end as cate_level1_name
                                ,c.cate_level2_name
                                ,b.goods_id
                                ,sum(case when cg.department in ('一部','二部','三部') and ((a.tru_pay_time>= '2019-06-01' and a.tru_pay_time < '2019-09-01' ) or (a.tru_pay_time>= '2020-04-01' and a.tru_pay_time < '2019-06-01' )) then b.original_goods_number*b.goods_price
                                            when cg.department in ('五部') and c.cate_level1_name = "Kid's Clothing" and ((a.tru_pay_time>= '2019-06-01' and a.tru_pay_time < '2019-09-01' ) or (a.tru_pay_time>= '2020-04-01' and a.tru_pay_time < '2019-06-01' )) then b.original_goods_number*b.goods_price
                                        else b.original_goods_number*b.goods_price end) revenue
                            from dw.dw_order_goods_fact b 
                            inner join (select a.*,case when a.pay_id=41 then a.pay_time else a.result_pay_time end as tru_pay_time from dw.dw_order_fact a ) a on a.order_id=b.order_id
                            left join dim.dim_goods c on c.goods_id=b.goods_id
                            left join jolly.who_esoloo_supplier  e on  c.provider_code = e.code
                            left join dim.dim_goods_category_group_new as cat
                            on c.cate_level1_name = cat.cate_level1_name
                            left join zybiro.bi_longjy_category_group_new as cg
                            on c.cate_level1_name = cg.cate_level1_name
                            where ((a.tru_pay_time>= '2020-01-01' and a.tru_pay_time < '2020-06-01') or (a.tru_pay_time>= '2019-06-01' and a.tru_pay_time < '2019-09-01'))
                            and a.site_id  in(400,600,700,601,900) 
                            and a.pay_status in(1,3)
                            and c.supplier_genre != 11
                            and cg.department != "七部"
                            group by 1,2,3,4,5
                        ) as gs
                    ) as a0
                ) as a1
                where a1.rn_sum/a1.cat1_gmv_sum <= 0.5
                union all
                select
                    a1.department
                    ,a1.category_group
                    ,a1.cate_level1_name
                    ,a1.cate_level2_name
                    ,a1.goods_id
                    ,a1.revenue
                    ,a1.rn
                from(
                    select
                        a0.department
                        ,a0.category_group
                        ,a0.cate_level1_name
                        ,a0.cate_level2_name
                        ,a0.goods_id
                        ,a0.revenue
                        ,a0.cat1_gmv_sum
                        ,a0.rn
                        ,sum(a0.revenue) over(partition by a0.category_group order by a0.category_group,rn) as rn_sum
                    from(
                        select
                            gs.department
                            ,gs.category_group
                            ,gs.cate_level1_name
                            ,gs.cate_level2_name
                            ,gs.goods_id
                            ,gs.revenue
                            ,sum(gs.revenue) over(partition by gs.category_group) as cat1_gmv_sum
                            ,row_number() over(partition by gs.category_group order by gs.revenue desc) as rn
                        from (  
                            select 
                                cg.department
                                ,cat.category_group
                                ,c.cate_level1_name
                                ,c.cate_level2_name
                                ,b.goods_id
                                ,sum(b.original_goods_number*b.goods_price) revenue
                            from dw.dw_order_goods_fact b 
                            inner join (select a.*,case when a.pay_id=41 then a.pay_time else a.result_pay_time end as tru_pay_time from dw.dw_order_fact a ) a on a.order_id=b.order_id
                            left join dim.dim_goods c on c.goods_id=b.goods_id
                            left join jolly.who_esoloo_supplier  e on  c.provider_code = e.code
                            left join dim.dim_goods_category_group_new as cat
                            on c.cate_level1_name = cat.cate_level1_name
                            left join zybiro.bi_longjy_category_group_new as cg
                            on c.cate_level1_name = cg.cate_level1_name
                            where a.tru_pay_time>= '2020-04-01' and a.tru_pay_time < from_timestamp(date_sub(now(),1), "yyyyMMdd")
                            and a.site_id  in(400,600,700,601,900) 
                            and a.pay_status in(1,3)
                            and c.supplier_genre != 11
                            and cg.department = "七部"
                            group by 1,2,3,4,5
                        ) as gs
                    ) as a0
                ) as a1
                where a1.rn_sum/a1.cat1_gmv_sum <= 0.5
            ) a2 
        )
        
        select
            a1.department as `部`
            ,a1.category_group as `类目`
            ,a1.cate_level1_name as `一级`
            ,a1.cate_level2_name as `二级`
            ,a1.month_week_lable as `周`
            ,a1.goods_num as `数量`
        from(
            select
                mjs.department
                ,mjs.category_group
                ,mjs.cate_level1_name
                ,mjs.cate_level2_name
                ,"0TOP款数量" as month_week_lable
                ,count(mjs.goods_id) as goods_num
            from month_jc_self as mjs
            group by 1,2,3,4
            union all
            select
                a0.department
                ,a0.category_group
                ,a0.cate_level1_name
                ,a0.cate_level2_name
                ,a0.month_week_lable
                ,count(distinct a0.goods_base) as goods_num
            from(
                select
                    mjs.department
                    ,mjs.category_group
                    ,mjs.cate_level1_name
                    ,mjs.cate_level2_name
                    ,mjs.goods_id as goods_base
                    ,gw.goods_id as goods_week
                    ,gw.data_date0
                    ,gw.month0
                    ,gw.week_year
                    ,min(gw.week_year) over(partition by gw.month0) as month_week_year
                    ,concat(cast(gw.month0 as string),"月","第", cast(gw.week_year - min(gw.week_year) over(partition by gw.month0) + 1 as string),"周") as month_week_lable
                from month_jc_self as mjs
                left join (
                    select
                        dg.goods_id
                        ,from_unixtime(unix_timestamp(data_date,"yyyyMMdd"),'yyyy-MM-dd') as data_date0
                        ,month(from_unixtime(unix_timestamp(data_date,"yyyyMMdd"),'yyyy-MM-dd')) as month0
                        ,case when weekofyear(from_unixtime(unix_timestamp(data_date,"yyyyMMdd"),'yyyy-MM-dd'))=18 then 19
                                else weekofyear(from_unixtime(unix_timestamp(data_date,"yyyyMMdd"),'yyyy-MM-dd')) 
                            end as week_year
                        ,count(dg.goods_id) as goods_num
                    from dim.dim_goods as dg
                    left join dim.dim_goods_extend as dge
                    on dg.goods_id = dge.goods_id
                    left join zybiro.bi_longjy_category_group_new as cg
                    on cg.cate_level1_name = cg.cate_level1_name
                    where dge.data_date >= '20200501'
                    and dge.data_date <= from_timestamp(date_sub(now(),1), "yyyyMMdd")
                    and dge.is_jc_on_sale = 1
                    and dg.supplier_genre != 11
                    group by 1,2,3,4
                ) as gw
                on mjs.goods_id = gw.goods_id
            ) as a0
            where a0.month_week_lable is not null
            group by 1,2,3,4,5
        ) as a1
        order by a1.department
            ,a1.category_group
            ,a1.cate_level1_name
            ,a1.cate_level2_name
            ,a1.month_week_lable
        """
        return sqlmsg

    def week_user_resale_class1(self, department):
        """
        自营各一级类目用户复购
        :return:
        """
        sqlmsg = """
        with day_user_avg as (
            select
                a0.category_group
                ,a0.cate_level1_name
                ,sum(case when a0.data_date >= from_timestamp(date_sub(now(),7),'yyyyMMdd') and a0.data_date <= from_timestamp(date_sub(now(),1),'yyyyMMdd') then user_num else 0 end)/7 as this_week_user_num
                ,sum(case when a0.data_date >= from_timestamp(date_sub(now(),14),'yyyyMMdd') and a0.data_date <= from_timestamp(date_sub(now(),7),'yyyyMMdd') then user_num else 0 end)/7 as last_week_user_num
                ,sum(case when a0.data_date >= from_timestamp(years_sub(date_sub(now(),7),1),'yyyyMMdd') and a0.data_date <= from_timestamp(years_sub(date_sub(now(),1),1),'yyyyMMdd') then user_num else 0 end)/7 as last_year_user_num
            from(
                select
                    from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') data_date,
                    cat.category_group,
                    case when c.cate_level1_name="Beauty" then c.cate_level2_name else c.cate_level1_name end as cate_level1_name,
                    count(distinct a.user_id) as user_num,
                    sum(b.original_goods_number) num,
                    sum(b.original_goods_number*b.goods_price) revenue
                from dw.dw_order_goods_fact b 
                inner join  dw.dw_order_fact a on a.order_id=b.order_id
                left join dim.dim_goods c on c.goods_id=b.goods_id
                left join jolly.who_esoloo_supplier  e on  c.provider_code = e.code
                left join dim.dim_goods_category_group_new as cat
                on c.cate_level1_name = cat.cate_level1_name
                left join zybiro.bi_longjy_category_group_new as cg
                on c.cate_level1_name = cg.cate_level1_name
                where
                ( (from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >= from_timestamp(date_sub(now(),7),'yyyyMMdd')
                    and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') <= from_timestamp(date_sub(now(),1),'yyyyMMdd'))
                or (from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >= from_timestamp(date_sub(now(),14),'yyyyMMdd')
                    and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') <= from_timestamp(date_sub(now(),7),'yyyyMMdd'))
                or (from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >= from_timestamp(years_sub(date_sub(now(),7),1),'yyyyMMdd')
                    and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') <= from_timestamp(years_sub(date_sub(now(),1),1),'yyyyMMdd'))
                )
                and a.site_id  in(400,600,700,601,900) 
                and a.pay_status in(1,3)
                and c.supplier_genre != 11
                and cg.department = "{cat}"
                group by 1,2,3
            ) as a0
            group by 1,2
        ),
        
        month_user_num as (
            select
                cat.category_group,
                case when c.cate_level1_name="Beauty" then c.cate_level2_name else c.cate_level1_name end as cate_level1_name,
                count(distinct case when from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >= from_timestamp(date_sub(now(),30),'yyyyMMdd') 
                                and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') <= from_timestamp(date_sub(now(),1),'yyyyMMdd') 
                            then a.user_id else null end) as this_month_user_num0,
                count(distinct case when from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >= from_timestamp(date_sub(now(),60),'yyyyMMdd')
                                and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') <= from_timestamp(date_sub(now(),30),'yyyyMMdd')
                            then a.user_id else null end) as last_month_user_num0,
                count(distinct case when from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >= from_timestamp(years_sub(date_sub(now(),30),1),'yyyyMMdd')
                                and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') <= from_timestamp(years_sub(date_sub(now(),1),1),'yyyyMMdd')
                            then a.user_id else null end) as last_year_user_num0
            from dw.dw_order_goods_fact b 
            inner join  dw.dw_order_fact a on a.order_id=b.order_id
            left join dim.dim_goods c on c.goods_id=b.goods_id
            left join jolly.who_esoloo_supplier  e on  c.provider_code = e.code
            left join dim.dim_goods_category_group_new as cat
            on c.cate_level1_name = cat.cate_level1_name
            left join zybiro.bi_longjy_category_group_new as cg
            on c.cate_level1_name = cg.cate_level1_name
            where
            ( (from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >= from_timestamp(date_sub(now(),30),'yyyyMMdd')
                and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') <= from_timestamp(date_sub(now(),1),'yyyyMMdd'))
            or (from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >= from_timestamp(date_sub(now(),60),'yyyyMMdd')
                and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') <= from_timestamp(date_sub(now(),30),'yyyyMMdd'))
            or (from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >= from_timestamp(years_sub(date_sub(now(),30),1),'yyyyMMdd')
                and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') <= from_timestamp(years_sub(date_sub(now(),1),1),'yyyyMMdd'))
            )
            and a.site_id  in(400,600,700,601,900) 
            and a.pay_status in(1,3)
            and c.supplier_genre != 11
            and cg.department = "{cat}"
            group by 1,2
        ),
        
        user_sale_agin as (
            select
                a1.category_group
                ,a1.cate_level1_name
                ,count(case when week0 = "this_month" then user_id else null end) as this_month_user_num
                ,count(case when week0 = "last_month" then user_id else null end) as last_month_user_num
                ,count(case when week0 = "last_year" then user_id else null end) as last_year_user_num
            from(
                select
                    a0.category_group
                    ,a0.cate_level1_name
                    ,case when data_date >= from_timestamp(date_sub(now(),30),'yyyyMMdd') and data_date <= from_timestamp(date_sub(now(),1),'yyyyMMdd') then "this_month"
                        when data_date >= from_timestamp(date_sub(now(),60),'yyyyMMdd') and data_date <= from_timestamp(date_sub(now(),30),'yyyyMMdd') then "last_month"
                        when data_date >= from_timestamp(years_sub(date_sub(now(),30),1),'yyyyMMdd') and data_date <= from_timestamp(years_sub(date_sub(now(),1),1),'yyyyMMdd') then "last_year"
                    else null end as week0
                    ,a0.user_id
                    ,count(a0.user_id) as user_sale_num
                from(
                    select
                        from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') data_date,
                        cat.category_group,
                        case when c.cate_level1_name="Beauty" then c.cate_level2_name else c.cate_level1_name end as cate_level1_name,
                        a.user_id
                    from dw.dw_order_goods_fact b 
                    inner join  dw.dw_order_fact a on a.order_id=b.order_id
                    left join dim.dim_goods c on c.goods_id=b.goods_id
                    left join jolly.who_esoloo_supplier  e on  c.provider_code = e.code
                    left join dim.dim_goods_category_group_new as cat
                    on c.cate_level1_name = cat.cate_level1_name
                    left join zybiro.bi_longjy_category_group_new as cg
                    on c.cate_level1_name = cg.cate_level1_name
                    where
                    ( (from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >= from_timestamp(date_sub(now(),30),'yyyyMMdd')
                        and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') <= from_timestamp(date_sub(now(),1),'yyyyMMdd'))
                    or (from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >= from_timestamp(date_sub(now(),60),'yyyyMMdd')
                        and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') <= from_timestamp(date_sub(now(),30),'yyyyMMdd'))
                    or (from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >= from_timestamp(years_sub(date_sub(now(),30),1),'yyyyMMdd')
                        and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') <= from_timestamp(years_sub(date_sub(now(),1),1),'yyyyMMdd')))
                    and a.site_id  in(400,600,700,601,900) 
                    and a.pay_status in(1,3)
                    and c.supplier_genre != 11
                    and cg.department = "{cat}"
                    group by 1,2,3,4
                ) as a0
                group by 1,2,3,4
            ) as a1
            where a1.user_sale_num > 1
            group by 1,2
        ),
        
        dau as (
            select
                cat.category_group,
                case when dg.cate_level1_name="Beauty" then dg.cate_level2_name else dg.cate_level1_name end as cate_level1_name,
                sum(case when dau.data_date >= from_timestamp(date_sub(now(),7),'yyyyMMdd') 
                        and dau.data_date <= from_timestamp(date_sub(now(),1),'yyyyMMdd') 
                    then dau.sales_uv else 0 end) / sum(case when dau.data_date >= from_timestamp(date_sub(now(),7),'yyyyMMdd') 
                        and dau.data_date <= from_timestamp(date_sub(now(),1),'yyyyMMdd') 
                    then dau.goods_click_uv else 0 end) as this_week_click_sale,
                
                sum(case when dau.data_date >= from_timestamp(date_sub(now(),14),'yyyyMMdd') 
                        and dau.data_date <= from_timestamp(date_sub(now(),7),'yyyyMMdd') 
                    then dau.sales_uv else 0 end) / sum(case when dau.data_date >= from_timestamp(date_sub(now(),14),'yyyyMMdd') 
                        and dau.data_date <= from_timestamp(date_sub(now(),7),'yyyyMMdd') 
                    then dau.goods_click_uv else 0 end) as last_week_click_sale,
                
                sum(case when dau.data_date >= from_timestamp(years_sub(date_sub(now(),7),1),'yyyyMMdd')
                        and dau.data_date <= from_timestamp(years_sub(date_sub(now(),1),1),'yyyyMMdd')
                    then dau.sales_uv else 0 end) / sum(case when dau.data_date >= from_timestamp(years_sub(date_sub(now(),7),1),'yyyyMMdd')
                        and dau.data_date <= from_timestamp(years_sub(date_sub(now(),1),1),'yyyyMMdd')
                    then dau.goods_click_uv else 0 end) as last_year_week_click_sale
            from rpt.rpt_sum_goods_daily as dau
            left join dim.dim_goods as dg
            on dau.goods_id = dg.goods_id
            left join dim.dim_goods_category_group_new as cat
            on dg.cate_level1_name = cat.cate_level1_name
            left join zybiro.bi_longjy_category_group_new as cg
            on dg.cate_level1_name = cg.cate_level1_name
            where (dau.data_date >= from_timestamp(date_sub(now(),7),'yyyyMMdd')
                and dau.data_date <= from_timestamp(date_sub(now(),1),'yyyyMMdd')
            or (dau.data_date >= from_timestamp(date_sub(now(),14),'yyyyMMdd')
                and dau.data_date <= from_timestamp(date_sub(now(),7),'yyyyMMdd'))
            or (dau.data_date >= from_timestamp(years_sub(date_sub(now(),7),1),'yyyyMMdd')
                and dau.data_date <= from_timestamp(years_sub(date_sub(now(),1),1),'yyyyMMdd')))
            and dau.site_id in (400,600,700,601,900)
            and dg.supplier_genre != 11
            and cg.department = "{cat}"
            group by 1,2
        )
        
        select
            dua.category_group
            ,dua.cate_level1_name
            ,dua.this_week_user_num as `本周日均购买用户量`
            ,dua.last_week_user_num as `上周日均购买用户量`
            ,dua.last_year_user_num as `同期日均购买用户量`
        
            ,usa.this_month_user_num/mun.this_month_user_num0 as `本月复购率`
            ,usa.last_month_user_num/mun.last_month_user_num0 as `上月复购率`
            ,usa.last_year_user_num/mun.last_year_user_num0 as `同期月复购率`
        
            ,dau.this_week_click_sale as `本周购买转化率`
            ,dau.last_week_click_sale as `上周购买转化率`
            ,dau.last_year_week_click_sale as `同期购买转化率`
        from day_user_avg as dua
        left join month_user_num as mun
        on dua.category_group = mun.category_group
        and dua.cate_level1_name = mun.cate_level1_name
        left join user_sale_agin as usa
        on dua.category_group = usa.category_group
        and dua.cate_level1_name = usa.cate_level1_name
        left join dau
        on dua.category_group = dau.category_group
        and dua.cate_level1_name = dau.cate_level1_name
        """.format(cat=department)
        return sqlmsg


if __name__ == '__main__':
    sqls = GoodsOnWeekSQL()
    sql_week_goods_on_all = sqls.week_user_resale_class1(department="一部")
    print(sql_week_goods_on_all)