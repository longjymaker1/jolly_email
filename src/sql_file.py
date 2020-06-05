#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/7/30 15:55
# @Author  : Long
# @Site    : 
# @File    : sql_file.py
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


def goods_sku_comment_return(category_group: str):
    sqlmsg = """
    with goods as (
        select
            cat.category_group,
            case when dg.cate_level1_name = "Beauty" then dg.cate_level2_name else dg.cate_level1_name end as cate_level1_name,
            case when dg.cate_level1_name = "Beauty" then dg.cate_level3_name else dg.cate_level2_name end as cate_level2_name,
            case when dg.cate_level1_name = "Beauty" then dg.cate_level4_name else dg.cate_level3_name end as cate_level3_name,
            dg.goods_id,
            dg.goods_name,
            skur.sku_id,
            case when dg.goods_season=1 then "春"
                when dg.goods_season=2 then "夏"
                when dg.goods_season=3 then "秋"
                when dg.goods_season=4 then "冬"
                when dg.goods_season=5 then "春夏"
                when dg.goods_season=6 then "春秋"
                when dg.goods_season=7 then "春冬"
                when dg.goods_season=8 then "夏秋"
                when dg.goods_season=9 then "夏冬"
                when dg.goods_season=10 then "秋冬"
                else cast(dg.goods_season as string) end as season,
            dg.provider_code,
            dg.supplier_genre,
            case when dg.is_forever_offsale=0 then "暂时下架"
                when dg.is_forever_offsale=1 then "永久下架"
                else null end as is_forever_offsale,
            case when dg.last_sold_out_reason = 1 then "销量差"
                when dg.last_sold_out_reason = 2 then "投诉率高"
                when dg.last_sold_out_reason = 3 then "暂时缺货"
                when dg.last_sold_out_reason = 4 then "永久缺货"
                when dg.last_sold_out_reason = 5 then "侵权"
                when dg.last_sold_out_reason = 6 then "重款"
                when dg.last_sold_out_reason = 7 then "其他"
                when dg.last_sold_out_reason = 8 then "涨价"
                when dg.last_sold_out_reason = 9 then "质量差"
                when dg.last_sold_out_reason = 10 then "色差严重"
                when dg.last_sold_out_reason = 11 then "取消合作"
                when dg.last_sold_out_reason = 12 then "尺寸超标"
                when dg.last_sold_out_reason = 13 then "临时禁运"
                else null end as last_sold_out_reason
        from jolly.who_inventory_sku_relation as skur
        left join dim.dim_goods as dg
        on skur.goods_id = dg.goods_id
        left join dim.dim_goods_extend as dge
        on dg.goods_id = dge.goods_id
        left join dim.dim_goods_category_group_new as cat
        on dg.cate_level1_name = cat.cate_level1_name
        where cat.category_group in ('beauty', '家居', '孕婴童用品', '婴童时尚')
        and dge.data_date = from_timestamp(date_sub(now(),1),'yyyyMMdd')
        and dg.supplier_genre != 11
        and dge.is_jc_on_sale=1
    ),

    sales as (
        select
            a0.goods_id,
            a0.sku_id,
            sum(num) as num_60,
            sum(case when data_date >= from_timestamp(date_sub(now(),30),'yyyyMMdd') then num else 0 end) as num_30
        from(
            select
                from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') data_date,
                c.goods_id,
                b.sku_id,
                sum(b.original_goods_number) num
            from dw.dw_order_goods_fact b 
            inner join  dw.dw_order_fact a on a.order_id=b.order_id
            left join dim.dim_goods c on c.goods_id=b.goods_id
            left join jolly.who_esoloo_supplier  e on  c.provider_code = e.code
            left join dim.dim_goods_category_group_new as cat
            on c.cate_level1_name = cat.cate_level1_name
            where
            from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >= from_timestamp(date_sub(now(),60),'yyyyMMdd')
            and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') <= from_timestamp(date_sub(now(),1),'yyyyMMdd')
            and cat.category_group in ('beauty', '家居', '孕婴童用品', '婴童时尚')
            and a.site_id  in(400,600,700,601,900) 
            and a.pay_status in(1,3)
            and c.supplier_genre != 11
            group by 1,2,3
        ) as a0
        -- where sku_id =4538362
        group by 1,2
    ),

    goods_comments as (
        select
            sga.goods_id,
            sga.goods_reason_return_30d,
            sga.goods_reason_return_rate_30d,
            sga.goods_reason_return_180d,
            sga.goods_reason_return_rate_180d,
            sga.bad_comments_rate
        from zybiro.bi_chili_syb_goods_all as sga
        left join dim.dim_goods as dg on dg.goods_id = sga.goods_id
        left join dim.dim_goods_category_group_new as cat
        on dg.cate_level1_name = cat.cate_level1_name
        where cat.category_group in ('beauty', '家居', '孕婴童用品', '婴童时尚')
        and sga.ds = from_timestamp(date_sub(now(),1),'yyyyMMdd')
        and dg.supplier_genre != 11
    ),

    goods_check as (
        select
            a0.goods_id,
            sum(checknums) as checknums_60,
            sum(case when data_date>=from_timestamp(date_sub(now(),30),'yyyyMMdd') then checknums else 0 end) as checknums_30
        from (
            select
                from_timestamp(case when b.pay_id=41 then b.pay_time else b.result_pay_time end,'yyyyMMdd') data_date,
                a.goods_id,
                sum(a.checknums) as checknums
            from zybiro.bi_damon_netprofit_2018 a  -- 统一采用damon备份表，下午才能更新
            inner join dw.dw_order_sub_order_fact b
            on a.order_id=b.order_id
            inner join dim.dim_goods p2
            on a.goods_id=p2.goods_id
            left join dim.dim_goods_category_group_new as cat
            on p2.cate_level1_name = cat.cate_level1_name
            where from_timestamp(case when b.pay_id=41 then b.pay_time else b.result_pay_time end,'yyyyMMdd')>=from_timestamp(date_sub(now(),60),'yyyyMMdd')
            and from_timestamp(case when b.pay_id=41 then b.pay_time else b.result_pay_time end,'yyyyMMdd')< from_timestamp(date_sub(now(),1),'yyyyMMdd')
            and b.site_id in (400,700,600,900,601)
            and b.pay_status in(1,3)
            and p2.supplier_genre<>11  -- 剔除pop供应商
            and cat.category_group in ('beauty', '家居', '孕婴童用品', '婴童时尚')
            group by 
            1,2
        ) as a0
        group by 1
    )


    select
        goods.category_group as `类目`,
        goods.cate_level1_name as `一级`,
        goods.cate_level2_name as `二级`,
        goods.cate_level3_name as `三级`,
        goods.goods_id,
        goods.goods_name as `商品名称`,
        goods.sku_id,
        goods.season as `季节`,
        goods.provider_code as `供应商编码`,
        goods.supplier_genre as `供应商类型`,
        goods.is_forever_offsale as `下架类型`,
        goods.last_sold_out_reason as `下架原因`,
        sales.num_60 as `近60天销量`,
        sales.num_30 as `近30天销量`,
        cg.checknums_60 as `近60天签收数量`,
        cg.checknums_30 as `近30天签收数量`,
        com.goods_reason_return_30d as `近30天退货数量`,
        com.goods_reason_return_rate_30d as `近30天退货率`,
        com.goods_reason_return_180d as `近180天退货数量`,
        com.goods_reason_return_rate_180d as `近180天退货率`,
        com.bad_comments_rate as `差评率`
    from goods left join sales
    on goods.goods_id = sales.goods_id
    and goods.sku_id = sales.sku_id
    left join goods_check as cg
    on goods.goods_id = cg.goods_id
    left join goods_comments as com
    on goods.goods_id = com.goods_id
    where goods.category_group = "{cate}"
    """.format(cate=category_group)
    return sqlmsg


def goods_msg_tmp():
    sqlmsg = """
    select
        cat.category_group,
        dg.cate_level1_id,
        dg.cate_level1_name,
        dg.cate_level2_id,
        dg.cate_level2_name,
        dg.cate_level3_id,
        dg.cate_level3_name,
        dg.cate_level4_id,
        dg.cate_level4_name,
        dg.cate_level5_id,
        dg.cate_level5_name,
        sku1.goods_id,
        sku1.rec_id,
        sku1.in_price,
        sku1.in_price_usd
    from ods.ods_who_sku_relation as sku1
    left join dim.dim_goods as dg
    on sku1.goods_id = dg.goods_id
    left join dim.dim_goods_category_group_new as cat
    on dg.cate_level1_name = cat.cate_level1_name
    where sku1.data_date = '20200224'
    -- and cat.category_group in ('家居','beauty','孕婴童用品','婴童时尚')
    and cat.category_group = '婴童时尚'
    and dg.supplier_genre != 11
    and dg.is_forever_offsale != 1
    """
    return sqlmsg


def goods_create_2020():
    sqlmsg = """
    with goods as (
        select
            dg.cate_level1_name,
            dg.goods_id,
            dg.goods_name,
            dge.in_price,
            dge.in_price_usd,
            dge.shop_price_1,
            dge.prst_price_1
        from dim.dim_goods as dg
        left join dim.dim_goods_extend as dge
        on dg.goods_id = dge.goods_id
        left join zybiro.bi_longjy_create_goods_tmp as tmp
        on dg.goods_id = tmp.goods_id
        where dge.data_date = from_timestamp(date_sub(now(),1), "yyyyMMdd")
        and tmp.goods_id is not null
    ),


    sales as (
        select
            b.goods_id,
            sum(b.original_goods_number) num,
            sum(b.original_goods_number*b.goods_price) revenue,
            sum(case when from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') = from_timestamp(date_sub(now(),1), "yyyyMMdd")
                    then b.original_goods_number else 0 end) as num_yesterday,
            sum(case when from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') = from_timestamp(date_sub(now(),1), "yyyyMMdd")
                    then b.original_goods_number*b.goods_price else 0 end) as revenue_yesterday
        from dw.dw_order_goods_fact b 
        inner join  dw.dw_order_fact a on a.order_id=b.order_id
        left join dim.dim_goods c on c.goods_id=b.goods_id
        left join jolly.who_esoloo_supplier  e on  c.provider_code = e.code
        left join dim.dim_goods_category_group_new as cat
        on c.cate_level1_name = cat.cate_level1_name
        left join zybiro.bi_longjy_create_goods_tmp as tmp
        on c.goods_id = tmp.goods_id
        where a.site_id  in(400,600,700,601,900) 
        and a.pay_status in(1,3)
        and c.supplier_genre != 11
        and cat.category_group in ('家居','beauty', '大件家居', '孕婴童用品', '婴童时尚')
        and tmp.goods_id is not null
        group by 1
    ),

    stocks as (
        select
            gstd.goods_id,
            sum(gstd.total_stock_num) as total_stock_num,
            sum(case when gstd.depot_id not in (15,16) then total_stock_num else 0 end) as dep_total_stock_num,
            sum(case when gstd.depot_id = 16 then total_stock_num else 0 end) as sa_total_stock_num,
            sum(case when gstd.depot_id = 15 then total_stock_num else 0 end) as uae_total_stock_num,
            sum(gstd.free_stock_num) as free_stock_num,
            sum(case when gstd.depot_id not in (15,16) then free_stock_num else 0 end) as dep_free_stock_num,
            sum(case when gstd.depot_id = 16 then free_stock_num else 0 end) as sa_free_stock_num,
            sum(case when gstd.depot_id = 15 then free_stock_num else 0 end) as uae_free_stock_num
        from ods.ods_who_wms_goods_stock_total_detail as gstd
        left join dim.dim_goods as dg on gstd.goods_id = dg.goods_id
        left join dim.dim_goods_extend as dge 
        on dg.goods_id = dge.goods_id
        and gstd.data_date = dge.data_date
        left join dim.dim_goods_category_group_new as cat
        on dg.cate_level1_name = cat.cate_level1_name
        left join zybiro.bi_longjy_create_goods_tmp as tmp
        on dg.goods_id = tmp.goods_id
        where cat.category_group in ('家居','beauty', '大件家居', '孕婴童用品', '婴童时尚')
        and gstd.data_date = from_timestamp(date_sub(now(),1), "yyyyMMdd")
        and tmp.goods_id is not null
        group by 1
    ),

    dau as (
        select
            dau.data_date,
            dau.goods_id,
            sum(dau.expo_pv) as expo_pv,
            sum(dau.expo_uv) as expo_uv,
            sum(dau.goods_click) as goods_click,
            sum(dau.goods_click_uv) as goods_click_uv,
            sum(dau.cart_click_uv) as cart_click_uv,
            sum(dau.bill_uv) as bill_uv,
            sum(dau.sales_uv) as sales_uv
        from rpt.rpt_sum_goods_daily as dau
        left join zybiro.bi_longjy_create_goods_tmp as tmp
        on dau.goods_id = tmp.goods_id
        where dau.data_date = from_timestamp(date_sub(now(),1), "yyyyMMdd")
        and dau.site_id in (400,600,700,601,900)
        and tmp.goods_id is not null
        group by 1,2
    )



    select
        goods.cate_level1_name as `一级类目`,
        goods.goods_id,
        goods.goods_name as `商品名称`,
        goods.in_price as `成本(￥)`,
        goods.in_price_usd as `成本($)`,
        goods.prst_price_1 as `售价`,
        goods.shop_price_1 as `吊牌价`,
        dau.expo_pv as `曝光pv`,
        dau.goods_click as `点击pv`,
        dau.goods_click_uv as `点击uv`,
        dau.cart_click_uv as `加购uv`,
        dau.bill_uv as `下单uv`,
        dau.sales_uv as `购买uv`,
        dau.goods_click/dau.expo_pv as `曝光点击率`,
        dau.cart_click_uv/dau.goods_click_uv as `详情加购转化率`,
        dau.bill_uv/dau.cart_click_uv as `加购下单转化率`,
        dau.sales_uv/dau.bill_uv as `下单支付转化率`,
        dau.sales_uv/dau.goods_click as `用户转化率`,
        (dau.expo_pv/1000)/sales.revenue_yesterday as `千次曝光价值`,
        dau.goods_click/sales.revenue_yesterday as `点击价值`,
        sales.revenue as `历史GMV`,
        sales.revenue_yesterday as `昨天GMV`,
        sales.num as `历史销量`,
        sales.num_yesterday as `昨天销量`,
        stocks.total_stock_num as `总库存`,
        stocks.dep_total_stock_num as `国内仓库存`,
        stocks.sa_total_stock_num as `sa库存`,
        stocks.uae_total_stock_num as `uae库存`,
        stocks.free_stock_num as `自由库存`,
        stocks.dep_free_stock_num as `国内仓自由库存`,
        stocks.sa_free_stock_num as `sa自由库存`,
        stocks.uae_free_stock_num as `uae自由库存`
    from goods left join dau on goods.goods_id = dau.goods_id
    left join sales on goods.goods_id = sales.goods_id
    left join stocks
    on goods.goods_id = stocks.goods_id
    """
    return sqlmsg


goods_pop_sql = """
with on_sales as   			
(				
    select
        data_date,
        cat.category_group,
        case when c.cate_level1_name='Beauty' then c.cate_level2_name else c.cate_level1_name end as cate_level1_name,
        count(ws.goods_id) sals_num
    from  dim.dim_goods_extend ws
    left join dim.dim_goods c on ws.goods_id =c.goods_id
    left join dim.dim_goods_category_group_new as cat
    on c.cate_level1_name = cat.cate_level1_name
    where cat.category_group in ('家居','母婴','童玩','beauty','婴童时尚','孕婴童用品')
    and ((ws.data_date >=from_timestamp(months_sub(trunc(now(),'Q'),1),'yyyyMMdd')
        and ws.data_date <=from_timestamp(date_sub(now(),1),'yyyyMMdd') )
    or(ws.data_date >=from_timestamp(months_sub(trunc(now(),'Q'),13),'yyyyMMdd')
        and ws.data_date <=from_timestamp(months_sub(date_sub(now(),1),12),'yyyyMMdd') ))
    and ws.is_jc_on_sale = 1
    group by 1,2,3
),

---商品的销售事实数据
sales_info as
(
    select
        from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') data_date,
        cat.category_group,
        case when c.cate_level1_name='Beauty' then c.cate_level2_name else c.cate_level1_name end as cate_level1_name,
        sum(b.original_goods_number) num,
        sum(b.original_goods_number*b.goods_price) revenue,
        sum(b.original_goods_number*b.in_price_usd) +nvl(sum( case when ((lower(a.country_name)='saudi arabia' and e.is_sa_supplier<>1) 
            or  (lower(a.country_name)='united arab emirates' and e.supplier_genre<>10)) then b.goods_price*b.original_goods_number*0.05
            when lower(a.country_name) in ('saudi arabia','united arab emirates')
            then  (b.goods_price-b.in_price_usd)*b.original_goods_number/1.05*0.05 end),0) cost_with_vat
    from dw.dw_order_goods_fact b 
    inner join  dw.dw_order_fact a on a.order_id=b.order_id
    left join dim.dim_goods c on c.goods_id=b.goods_id
    left join jolly.who_esoloo_supplier  e on  c.provider_code = e.code
    left join dim.dim_goods_category_group_new as cat
    on c.cate_level1_name = cat.cate_level1_name
    where
    ((from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >= from_timestamp(months_sub(trunc(now(),'Q'),1),'yyyyMMdd')
        and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') <= from_timestamp(date_sub(now(),1),'yyyyMMdd') )
    or (from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >=from_timestamp(months_sub(trunc(now(),'Q'),13),'yyyyMMdd')
    and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') <=from_timestamp(months_sub(date_sub(now(),1),12),'yyyyMMdd') ))
    and a.site_id  in(400,600,700,601,900) 
    and a.pay_status in(1,3)
    and cat.category_group in ('家居','母婴','童玩','beauty','婴童时尚','孕婴童用品')
    group by
    1,2,3
),
pri_in_come as 
(
   select
        regexp_replace(pay_date,'-','') data_date,
        cat.category_group,
        case when g.cate_level1_name='Beauty' then g.cate_level2_name else g.cate_level1_name end as cate_level1_name,
        sum(profits2) pre_income
    from zybiro.bi_faye_net_profit_precast_new a
    left join dim.dim_goods g on a.goods_id=g.goods_id
    left join zybiro.bi_chili_cat_group p3
    on g.cate_level1_name=p3.cat_level1_name
    left join dim.dim_goods_category_group_new as cat
    on g.cate_level1_name = cat.cate_level1_name
    where data_date = from_timestamp(date_sub(now(),1),'yyyyMMdd')
    and regexp_replace(pay_date,'-','') > from_timestamp(date_sub(trunc(now(),'MM'),1),'yyyyMMdd')
    and cat.category_group in ('家居','母婴','童玩','beauty','婴童时尚','孕婴童用品')
    group by 1,2,3
    union
    select 
        regexp_replace(substr(case when b.pay_id=41 then b.pay_time else b.result_pay_time end,1,10 ),'-','') data_date,
        cat.category_group,
        case when p2.cate_level1_name='Beauty' then p2.cate_level2_name else p2.cate_level1_name end as cate_level1_name,
        sum(a.income)+sum(a.discountamount)-sum(a.cost)-sum(a.newshippingfees)-sum(a.thedepotfees)-sum(vat)-sum(a.duty) pre_income-- 净利额
    from zybiro.bi_damon_netprofit_2018 a  -- 统一采用damon备份表，下午才能更新
    inner join dw.dw_order_sub_order_fact b
    on a.order_id=b.order_id
    inner join dim.dim_goods p2
    on a.goods_id=p2.goods_id
    left join dim.dim_goods_category_group_new as cat
    on p2.cate_level1_name = cat.cate_level1_name
    where regexp_replace(substr(case when b.pay_id=41 then b.pay_time else b.result_pay_time end,1,10 ),'-','') >= from_timestamp(months_sub(trunc(now(),'Q'),1),'yyyyMMdd')
    and regexp_replace(substr(case when b.pay_id=41 then b.pay_time else b.result_pay_time end,1,10 ),'-','') <= from_timestamp(date_sub(trunc(now(),'MM'),1),'yyyyMMdd')
    and b.site_id in (400,700,600,900,601)
    and b.pay_status in(1,3)
    and cat.category_group in ('家居','母婴','童玩','beauty','婴童时尚','孕婴童用品')
    group by
    1,2,3
),

---商品的净利润情况 in_come_true as( ),
new_up as
(
    select
        regexp_replace(to_date(first_on_sale_time),'-','')  data_date,
        cat.category_group,
        case when dg.cate_level1_name='Beauty' then dg.cate_level2_name else dg.cate_level1_name end as cate_level1_name,
        count(dg.goods_id) new_up_num
    from
    dim.dim_goods as dg
    left join dim.dim_goods_extend as dge
    on dg.goods_id = dge.goods_id
    and dge.data_date = from_timestamp(date_sub(now(),1),'yyyyMMdd')
    left join dim.dim_goods_category_group_new as cat
    on dg.cate_level1_name = cat.cate_level1_name
    where regexp_replace(to_date(first_on_sale_time),'-','')>= from_timestamp(months_sub(trunc(now(),'Q'),1),'yyyyMMdd')
    and regexp_replace(to_date(first_on_sale_time),'-','')<=from_timestamp(date_sub(now(),1),'yyyyMMdd')
    and dge.is_jc_on_sale = 1
    and dge.is_jc_sale = 1
    and cat.category_group in ('家居','母婴','童玩','beauty','婴童时尚','孕婴童用品')
    group by 1,2,3				
)


select
    t1.*,
    t4.new_up_num,
    t2.num,
    t2.revenue,
    t2.cost_with_vat,
    t3.pre_income
from 
on_sales  t1
left join sales_info  t2 on t1.category_group =t2.category_group and t1.cate_level1_name=t2.cate_level1_name
and t1.data_date=t2.data_date
left join pri_in_come t3  on t1.cate_level1_name=t3.cate_level1_name
and t1.data_date=t3.data_date  ---t1.category_group =t3.category_group and
left join new_up t4 on t1.category_group =t4.category_group and t1.cate_level1_name=t4.cate_level1_name
and t1.data_date=t4.data_date;
"""

category_pop_sql = """
select
    from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') data_date,
    p3.category_group_1,
    c.cate_level1_name,
    sum(b.original_goods_number*b.goods_price) revenue
from dw.dw_order_goods_fact b 
inner join  dw.dw_order_fact a on a.order_id=b.order_id
left join dim.dim_goods c on c.goods_id=b.goods_id
left join jolly.who_esoloo_supplier  e on  c.provider_code = e.code
left join zybiro.bi_chili_cat_group p3 on c.cate_level1_name=p3.cat_level1_name
where
from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >='{begin}'
and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') <='{end}'
and a.site_id  in(400,600,700,601,900) 
and a.pay_status in(1,3)
group by 1,2,3
""".format(begin=this_quarter_start, end=yestoday)

goods_no_pop_sql = """
with on_sales as   			
(				
    select
        data_date,
        c.category_group,
        case when c.cate_level1_name='Beauty' then c.cate_level2_name else c.cate_level1_name end as cate_level1_name,
        count(ws.goods_id) sals_num
    from  dim.dim_goods_extend ws
    left join dim.dim_goods c on ws.goods_id =c.goods_id
    where cat.category_group in ('家居','母婴','童玩','beauty','婴童时尚','孕婴童用品')
    and ((ws.data_date >=from_timestamp(months_sub(trunc(now(),'Q'),1),'yyyyMMdd')
        and ws.data_date <=from_timestamp(date_sub(now(),1),'yyyyMMdd') )
    or(ws.data_date >=from_timestamp(months_sub(trunc(now(),'Q'),13),'yyyyMMdd')
        and ws.data_date <=from_timestamp(months_sub(date_sub(now(),1),12),'yyyyMMdd') ))
    and ws.is_jc_on_sale = 1
    and c.supplier_genre<>11
    group by 1,2,3
),

---商品的销售事实数据
sales_info as
(
    select
        from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') data_date,
        c.category_group,
        case when c.cate_level1_name='Beauty' then c.cate_level2_name else c.cate_level1_name end as cate_level1_name,
        sum(b.original_goods_number) num,
        sum(b.original_goods_number*b.goods_price) revenue,
        sum(b.original_goods_number*b.in_price_usd) +nvl(sum( case when ((lower(a.country_name)='saudi arabia' and e.is_sa_supplier<>1) 
            or  (lower(a.country_name)='united arab emirates' and e.supplier_genre<>10)) then b.goods_price*b.original_goods_number*0.05
            when lower(a.country_name) in ('saudi arabia','united arab emirates')
            then  (b.goods_price-b.in_price_usd)*b.original_goods_number/1.05*0.05 end),0) cost_with_vat
    from dw.dw_order_goods_fact b 
    inner join  dw.dw_order_fact a on a.order_id=b.order_id
    left join dim.dim_goods c on c.goods_id=b.goods_id
    left join jolly.who_esoloo_supplier  e on  c.provider_code = e.code
    where
    ((from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >= from_timestamp(months_sub(trunc(now(),'Q'),1),'yyyyMMdd')
        and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') <= from_timestamp(date_sub(now(),1),'yyyyMMdd') )
    or (from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >=from_timestamp(months_sub(trunc(now(),'Q'),13),'yyyyMMdd')
    and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') <=from_timestamp(months_sub(date_sub(now(),1),12),'yyyyMMdd') ))
    and a.site_id  in(400,600,700,601,900) 
    and a.pay_status in(1,3)
    and c.category_group in ('家居','母婴','童玩','beauty','婴童时尚','孕婴童用品')
    and c.supplier_genre<>11
    group by
    1,2,3
),
pri_in_come as 
(
   select
        regexp_replace(pay_date,'-','') data_date,
        g.category_group,
        case when g.cate_level1_name='Beauty' then g.cate_level2_name else g.cate_level1_name end as cate_level1_name,
        sum(profits2) pre_income
    from zybiro.bi_faye_net_profit_precast_new a
    left join dim.dim_goods g on a.goods_id=g.goods_id
    left join zybiro.bi_chili_cat_group p3
    on g.cate_level1_name=p3.cat_level1_name
    where data_date = from_timestamp(date_sub(now(),1),'yyyyMMdd')
    and regexp_replace(pay_date,'-','') > from_timestamp(date_sub(trunc(now(),'MM'),1),'yyyyMMdd')
    and g.category_group in ('家居','母婴','童玩','beauty','婴童时尚','孕婴童用品')
    group by 1,2,3
    union
    select 
        regexp_replace(substr(case when b.pay_id=41 then b.pay_time else b.result_pay_time end,1,10 ),'-','') data_date,
        p2.category_group,
        case when p2.cate_level1_name='Beauty' then p2.cate_level2_name else p2.cate_level1_name end as cate_level1_name,
        sum(a.income)+sum(a.discountamount)-sum(a.cost)-sum(a.newshippingfees)-sum(a.thedepotfees)-sum(vat)-sum(a.duty) pre_income-- 净利额
    from zybiro.bi_damon_netprofit_2018 a  -- 统一采用damon备份表，下午才能更新
    inner join dw.dw_order_sub_order_fact b
    on a.order_id=b.order_id
    inner join dim.dim_goods p2
    on a.goods_id=p2.goods_id
    where regexp_replace(substr(case when b.pay_id=41 then b.pay_time else b.result_pay_time end,1,10 ),'-','') >= from_timestamp(months_sub(trunc(now(),'Q'),1),'yyyyMMdd')
    and regexp_replace(substr(case when b.pay_id=41 then b.pay_time else b.result_pay_time end,1,10 ),'-','') <= from_timestamp(date_sub(trunc(now(),'MM'),1),'yyyyMMdd')
    and b.site_id in (400,700,600,900,601)
    and b.pay_status in(1,3)
    and p2.supplier_genre<>11  -- 剔除pop供应商
    and p2.category_group in ('家居','母婴','童玩','beauty','婴童时尚','孕婴童用品')
    group by
    1,2,3
),

---商品的净利润情况 in_come_true as( ),
new_up as
(
    select
        regexp_replace(to_date(first_on_sale_time),'-','')  data_date,
        dg.category_group,
        case when dg.cate_level1_name='Beauty' then dg.cate_level2_name else dg.cate_level1_name end as cate_level1_name,
        count(dg.goods_id) new_up_num
    from
    dim.dim_goods as dg
    left join dim.dim_goods_extend as dge
    on dg.goods_id = dge.goods_id
    and dge.data_date = from_timestamp(date_sub(now(),1),'yyyyMMdd')
    where regexp_replace(to_date(first_on_sale_time),'-','')>= from_timestamp(months_sub(trunc(now(),'Q'),1),'yyyyMMdd')
    and regexp_replace(to_date(first_on_sale_time),'-','')<=from_timestamp(date_sub(now(),1),'yyyyMMdd')
    and supplier_genre<>11
    and dge.is_jc_on_sale = 1
    and dge.is_jc_sale = 1
    and dg.category_group in ('家居','母婴','童玩','beauty','婴童时尚','孕婴童用品')
    group by 1,2,3				
)


select
    t1.*,
    t4.new_up_num,
    t2.num,
    t2.revenue,
    t2.cost_with_vat,
    t3.pre_income
from 
on_sales  t1
left join sales_info  t2 on t1.category_group =t2.category_group and t1.cate_level1_name=t2.cate_level1_name
and t1.data_date=t2.data_date
left join pri_in_come t3  on t1.cate_level1_name=t3.cate_level1_name
and t1.data_date=t3.data_date  ---t1.category_group =t3.category_group and
left join new_up t4 on t1.category_group =t4.category_group and t1.cate_level1_name=t4.cate_level1_name
and t1.data_date=t4.data_date;
"""


def new_goods_no_pop_sql(category: str):
    """
    一级类目日报
    :param category: 类目
    :return: sql
    """
    sqlmsg = """
        with on_sales as
        (
            select
                data_date,
                cat.category_group,
                case when c.cate_level1_name='Beauty' then c.cate_level2_name else c.cate_level1_name end as cate_level1_name,
                count(ws.goods_id) sals_num
            from  dim.dim_goods_extend ws
            left join dim.dim_goods c on ws.goods_id =c.goods_id
            left join dim.dim_goods_category_group_new as cat on c.cate_level1_name=cat.cate_level1_name
            where ((ws.data_date >= from_timestamp(months_sub(trunc(now(),"MM"),2),'yyyyMMdd')
                and ws.data_date <= from_timestamp(date_sub(now(),1),'yyyyMMdd') )
            or(ws.data_date >= from_timestamp(years_sub(months_sub(trunc(now(),"MM"),2),1), "yyyyMMdd")
                and ws.data_date <= from_timestamp(years_sub(date_sub(now(),1),1), "yyyyMMdd") ))
            and ws.is_jc_on_sale = 1
            and c.supplier_genre<>11
            and cat.category_group in ("{cate}")
            group by 1,2,3
        ),
        
        ---商品的销售事实数据
        sales_info as
        (
            select
                from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') data_date,
                cat.category_group,
                case when c.cate_level1_name='Beauty' then c.cate_level2_name else c.cate_level1_name end as cate_level1_name,
                sum(b.original_goods_number) num,
                sum(b.original_goods_number*b.goods_price) revenue,
                sum(b.original_goods_number*b.in_price_usd) +nvl(sum( case when ((lower(a.country_name)='saudi arabia' and e.is_sa_supplier<>1) 
                    or  (lower(a.country_name)='united arab emirates' and e.supplier_genre<>10)) then b.goods_price*b.original_goods_number*0.05
                    when lower(a.country_name) in ('saudi arabia','united arab emirates')
                    then  (b.goods_price-b.in_price_usd)*b.original_goods_number/1.05*0.05 end),0) cost_with_vat
            from dw.dw_order_goods_fact b 
            inner join  dw.dw_order_fact a on a.order_id=b.order_id
            left join dim.dim_goods c on c.goods_id=b.goods_id
            left join jolly.who_esoloo_supplier  e on  c.provider_code = e.code
            left join dim.dim_goods_category_group_new as cat
            on c.cate_level1_name = cat.cate_level1_name
            where
            ((from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >= from_timestamp(months_sub(trunc(now(),"MM"),2),'yyyyMMdd')
                and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') <= from_timestamp(date_sub(now(),1),'yyyyMMdd') )
            or (from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >= from_timestamp(years_sub(months_sub(trunc(now(),"MM"),2),1), "yyyyMMdd")
            and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') <= from_timestamp(years_sub(date_sub(now(),1),1), "yyyyMMdd") ))
            and a.site_id  in(400,600,700,601,900) 
            and a.pay_status in(1,3)
            and cat.category_group in ("{cate}")
            and c.supplier_genre<>11
            group by
            1,2,3
        ),
        
        new_up as
        (
            select
                regexp_replace(to_date(first_on_sale_time),'-','')  data_date,
                cat.category_group,
                case when dg.cate_level1_name='Beauty' then dg.cate_level2_name else dg.cate_level1_name end as cate_level1_name,
                count(dg.goods_id) new_up_num
            from
            dim.dim_goods as dg
            left join dim.dim_goods_extend as dge
            on dg.goods_id = dge.goods_id
            and dge.data_date = from_timestamp(date_sub(now(),1),'yyyyMMdd')
            left join dim.dim_goods_category_group_new as cat
            on dg.cate_level1_name = cat.cate_level1_name
            where regexp_replace(to_date(first_on_sale_time),'-','') >= from_timestamp(months_sub(trunc(now(),"MM"),2),'yyyyMMdd')
            and regexp_replace(to_date(first_on_sale_time),'-','') <= from_timestamp(date_sub(now(),1),'yyyyMMdd')
            and supplier_genre<>11
            and dge.is_jc_on_sale = 1
            and dge.is_jc_sale = 1
            and cat.category_group in ("{cate}")
            group by 1,2,3
        ),

        income as (
            select  --当月使用预测净利润
            regexp_replace(pay_date,'-','') data_date
            ,cat.category_group
            ,case when g.cate_level1_name = "Beauty" then g.cate_level2_name else g.cate_level1_name end as cate_level1_name
            ,sum(profits2) pre_income
            from zybiro.bi_faye_net_profit_precast_new a
            left join dim.dim_goods g on a.goods_id=g.goods_id
            left join dim.dim_goods_category_group_new as cat
            on g.cate_level1_name = cat.cate_level1_name
            where data_date = regexp_replace(to_date(date_sub(current_timestamp(),1)),'-','')
            and regexp_replace(pay_date,'-','') = from_timestamp(date_sub(now(),1),'yyyyMMdd')
            and cat.category_group in ("{cate}")
            group by 1,2,3
            union
            select 
            regexp_replace(substr(case when b.pay_id=41 then b.pay_time else b.result_pay_time end,1,10 ),'-','') data_date,
            cat.category_group,
            case when p2.cate_level1_name = "Beauty" then p2.cate_level2_name else p2.cate_level1_name end as cate_level1_name,
            sum(a.income)+sum(a.discountamount)-sum(a.cost)-sum(a.newshippingfees)-sum(a.thedepotfees)-sum(vat)-sum(a.duty) pre_income-- 净利额
            from zybiro.bi_damon_netprofit_2018 a  -- 统一采用damon备份表，下午才能更新
            inner join dw.dw_order_sub_order_fact b
            on a.order_id=b.order_id
            inner join dim.dim_goods p2
            on a.goods_id=p2.goods_id
            left join dim.dim_goods_category_group_new as cat
            on p2.cate_level1_name = cat.cate_level1_name
            where ((regexp_replace(substr(case when b.pay_id=41 then b.pay_time else b.result_pay_time end,1,10 ),'-','')>= from_timestamp(months_sub(trunc(to_date(now()),"MM"),2),'yyyyMMdd')
                and regexp_replace(substr(case when b.pay_id=41 then b.pay_time else b.result_pay_time end,1,10 ),'-','')< from_timestamp(date_sub(now(),1),'yyyyMMdd'))
            or (regexp_replace(substr(case when b.pay_id=41 then b.pay_time else b.result_pay_time end,1,10 ),'-','')>= from_timestamp(years_sub(months_sub(trunc(to_date(now()),"MM"),2),1),'yyyyMMdd')
                and regexp_replace(substr(case when b.pay_id=41 then b.pay_time else b.result_pay_time end,1,10 ),'-','')< from_timestamp(years_sub(date_sub(now(),1),1),'yyyyMMdd')))
            and b.site_id in (400,700,600,900,601)
            and b.pay_status in(1,3)
            and p2.supplier_genre<>11  -- 剔除pop供应商
            and cat.category_group in ("{cate}")
            group by 
            1,2,3
        )
        
        
        select
            from_unixtime(unix_timestamp(t1.data_date, "yyyyMMdd"), "yyyy-MM-dd") as data_date,
            t1.category_group,
            t1.cate_level1_name,
            t1.sals_num,
            t4.new_up_num,
            t2.num,
            t2.revenue,
            t2.cost_with_vat,
            t5.pre_income
        from 
        on_sales  t1
        left join sales_info  t2 on t1.category_group =t2.category_group and t1.cate_level1_name=t2.cate_level1_name
        and t1.data_date=t2.data_date
        left join new_up t4 on t1.category_group =t4.category_group and t1.cate_level1_name=t4.cate_level1_name
        and t1.data_date=t4.data_date
        left join income as t5
        on t1.category_group = t5.category_group
        and t1.cate_level1_name = t5.cate_level1_name
        and t1.data_date = t5.data_date
        where t1.category_group in ("{cate}");
    """.format(cate=category)
    return sqlmsg


category_no_pop_sql = """
select
    from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') data_date,
    p3.category_group_1,
    c.cate_level1_name,
    sum(b.original_goods_number*b.goods_price) revenue
from dw.dw_order_goods_fact b 
inner join  dw.dw_order_fact a on a.order_id=b.order_id
left join dim.dim_goods c on c.goods_id=b.goods_id
left join jolly.who_esoloo_supplier  e on  c.provider_code = e.code
left join zybiro.bi_chili_cat_group p3 on c.cate_level1_name=p3.cat_level1_name
where
from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >='{begin}'
and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') <='{end}'
and a.site_id  in(400,600,700,601,900) 
and a.pay_status in(1,3)
and c.supplier_genre<>11
group by 1,2,3
""".format(begin=this_quarter_start, end=yestoday)

# on_sale_sql = """
# select t2.category_group
# ,t.*
# from zybiro.month_goods_on_sale_info t
# inner join dim.dim_goods t1 on t.goods_id=t1.goods_id
# inner join (select * from dim.dim_goods_category_group where category_group in ('婚庆周边','家居','母婴','童玩')) t2
# on t1.cate_level1_name=t2.cate_level1_name
# and t1.cate_level2_name=t2.cate_level2_name
# left join jolly.who_esoloo_supplier as wes
# on t1.provider_code = wes.code
# where wes.supplier_genre <> 11;
# """

on_sale_sql = """
select 
cats.category_group_1 as `品类组`
,t.goods_sn as `商品编码`
,t.goods_id as `商品id`
,t.goods_season as `季节`
,t.provide_code as `供应商编码`
,wes.great_time as `合作时间`
,case when wes.supplier_genre = 3 then '取消合作'
    when wes.supplier_genre = 4 then 'MV'
    when wes.supplier_genre = 5 then '日韩'
    when wes.supplier_genre = 6 then '沙特本地供应商'
    when wes.supplier_genre = 9 then '土耳其本地供应商'
    when wes.supplier_genre = 10 then '阿联酋其本地供应商'
    else null end as `状态`
,t.cat_level1_name as `一级`
,t.cat_level2_name as `二级`
,t.cat_level3_name as `三级`
,t.first_on_sale_time as `首次上架时间`
,t.shop_price as `原价`
,t.in_price as `成本价`
,t.on_sale_days as `在架时长`
,t.on_sale_conditon as `在架状态`
,t.prst_price as `促销现价`
,t.num_acc as `累计销量`
,t.revenue_acc as `累计销售额`
,t.sold_price_acc as `累计销售均价`
,t.order_num_month as `月订单数`
,t.num_month as `月销量`
,t.revenue_month as `月收入`
,t.diaopai_month as `月吊牌`
,t.chengben_month as `月成本`
,t.imp_month as `月曝光`
,t.click_month as `月点击`
,t.addtobag_month as `月加购`
,t.wishlist_month as `月收藏`
,t.tax_month as `税费`
,t.sold_price_month  as `月均售价`
from zybiro.month_goods_on_sale_info t  
inner join dim.dim_goods t1 on t.goods_id=t1.goods_id
left join zybiro.bi_chili_cat_group as cats on t1.cate_level1_name = cats.cat_level1_name
left join jolly.who_esoloo_supplier as wes on t.provide_code = wes.code
where t.provide_code in (
    select
       t.provide_code
    from zybiro.month_goods_on_sale_info as t
    inner join dim.dim_goods t1 on t.goods_id=t1.goods_id
    inner join (select * from dim.dim_goods_category_group where category_group in ('婚庆周边','家居','母婴','童玩')) t2 
    on t1.cate_level1_name=t2.cate_level1_name
    and t1.cate_level2_name=t2.cate_level2_name
)
and wes.supplier_genre <> 11
"""

KA_saler = """
select tt2.provider_code
,tt2.supp_name
,round(nvl(tt2.sales_amount_mon_ka,0),2) sales_volume_mon_ka   ---KA商家近1月销售额
,nvl(tt2.cnt_onsale_ka,0) cnt_onsale_ka  ---KA商家当前在架商品款数
,nvl(tt2.cnt_new_mon_ka,0) cnt_new_mon_ka  ---KA商家近1月上新数量
,nvl(tt2.cnt_goods_top_ka,0) cnt_goods_top_ka  ---KA商家TOP款数量
,round(nvl(tt2.sales_amount_top_ka,0),2) sales_amount_top_ka  ---KA商家TOP款销售额
,round(nvl(tt3.sales_amount_mon_cate,0),2) sales_amount_mon_cate ---母婴类目销售额
,concat(cast(cast(round(tt2.sales_amount_top_ka/tt3.sales_amount_mon_cate*100,2) as decimal(18,2)) as string),'%') ka_top_percent   ---KA商家TOP款销售额占比
,concat(cast(cast(round(tt2.sales_amount_mon_ka/tt3.sales_amount_mon_cate*100,2) as decimal(18,2)) as string),'%') ka_percent       ---KA商家销售额占比
from
(
	select '母婴童' category_group_1
	,tt.provider_code
	,tt1.supp_name
	,tt.sales_volume_mon_ka
	,tt.sales_amount_mon_ka
	,tt.cnt_onsale_ka
	,tt.cnt_new_mon_ka
	,tt.cnt_goods_top_ka
	,tt.sales_amount_top_ka
	from
	(
		select
		a.provider_code
		,a.sales_volume_mon_ka   ---KA商家近1月销量
		,a.sales_amount_mon_ka   ---KA商家近1月销售额
		,b.cnt_onsale_ka  ---KA商家当前在架商品款数
		,c.cnt_new_mon_ka  ---KA商家近1月上新数量
		,d.cnt_goods_top_ka ---KA商家TOP款数量
		,d.sales_volume_top_ka  ---KA商家TOP款销量
		,d.sales_amount_top_ka  ---KA商家TOP款销售额
		---,d.sales_volume_top_cate  ---类目TOP2000销量
		---,d.sales_amount_top_cate  ---类目TOP2000销售额
		from
		(
			select
			c.provider_code
			,sum(b.original_goods_number) sales_volume_mon_ka  ---KA商家近1月销量
			,sum(b.original_goods_number*b.goods_price) sales_amount_mon_ka  ---KA商家近1月销售额
			from
			dw.dw_order_goods_fact b inner join  dw.dw_order_fact a on a.order_id=b.order_id
			left outer join dim.dim_goods c on c.goods_id=b.goods_id
			left outer join zybiro.bi_chili_cat_group d on c.cate_level1_name=d.cat_level1_name
			where
			from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >=regexp_replace(to_date(date_sub(now(),30)),'-','')
			and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') <=regexp_replace(to_date(date_sub(now(),1)),'-','')
			and a.site_id  in(400,600,700,601,900)
			and a.pay_status in(1,3)
			---and c.provider_code in ('350','0TS','0WA','10E','12K','1LT','20U','28K','2D5','2L7','2L9','2YY','37N','3DG','3RP','3VN','3XE','4EN','4FC','4L9','4MR','57L','61L','66F','675','6B3','6DG','6UW','74L','7B5','7S0','82S','8J7','8TG')
			and d.category_group_1 in ('母婴童')
			group by 1
		) a
		---当前在架数量
		left outer join
		(
			select t1.provider_code
			,count(distinct t.goods_id) cnt_onsale_ka ---KA商家当前在架商品款数
			from
			(
				select distinct t.goods_id
				from dim.dim_goods_extend t
				where t.data_date=regexp_replace(to_date(date_sub(now(),1)),'-','')
				and t.is_jc_on_sale=1
			) t
			inner join dim.dim_goods t1 on t.goods_id=t1.goods_id
			inner join (select * from zybiro.bi_chili_cat_group where category_group_1 in ('母婴童')) t2 on t1.cate_level1_name=t2.cat_level1_name
			group by 1
		) b on a.provider_code=b.provider_code
		---近1月上新数量
		left outer join
		(
			select t.provider_code
			,count(distinct t.goods_id) cnt_new_mon_ka ---KA商家近1月上新数量
			from dim.dim_goods t
			inner join (select * from zybiro.bi_chili_cat_group where category_group_1 in ('母婴童')) t1
			on t.cate_level1_name=t1.cat_level1_name
			where to_date(first_on_sale_time)>=to_date(date_sub(now(),30))
			and to_date(first_on_sale_time)<=to_date(date_sub(now(),1))
			group by 1
		) c on a.provider_code=c.provider_code
		---KA商家TOP（2000）
		left outer join
		(
			select
			d.provider_code
			,count(distinct d.goods_id) cnt_goods_top_ka ---KA商家TOP款数量
			,sum(d.num) sales_volume_top_ka ---KA商家TOP款销量
			,sum(d.revenue) sales_amount_top_ka  ---KA商家TOP款销售额
			from
			(
				select
				b.goods_id
				,c.provider_code
				,sum(b.original_goods_number) num
				,sum(b.original_goods_number*b.goods_price) revenue
				from
				dw.dw_order_goods_fact b inner join  dw.dw_order_fact a on a.order_id=b.order_id
				left outer join dim.dim_goods c on c.goods_id=b.goods_id
				left outer join zybiro.bi_chili_cat_group d on c.cate_level1_name=d.cat_level1_name
				where
				from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >=regexp_replace(to_date(date_sub(now(),30)),'-','')
				and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') <=regexp_replace(to_date(date_sub(now(),1)),'-','')
				and a.site_id  in(400,600,700,601,900)
				and a.pay_status in(1,3)
				and d.category_group_1 in ('母婴童')
				group by 1,2
				order by revenue desc
				limit 2000
			) d
			group by 1
		) d on a.provider_code=d.provider_code
	) tt
	left outer join
	(
		select distinct t.code
		,t.supp_name
		from secure.who_esoloo_supplier t
	) tt1 on tt.provider_code=tt1.code
	--- where tt.provider_code in ('37N','93J','82S','66F','3XE','57L','2L9','2D5','8TG','6DG','48G','3RP',
	---                           '28K','4FC','675','61L','0TS','6UW','0WA','4L9','350','3DG','7B5','6B3',
	---                           '7S0','8Z5','8J7','12K','74L','3VN','6QH','7K0','63W','7LU')
) tt2
---母婴童类目
left outer join
(
	select
	d.category_group_1
	,sum(b.original_goods_number) sales_volume_mon_cate  ---类目销量
	,sum(b.original_goods_number*b.goods_price) sales_amount_mon_cate  ---类目销售额
	from
	dw.dw_order_goods_fact b inner join  dw.dw_order_fact a on a.order_id=b.order_id
	left outer join dim.dim_goods c on c.goods_id=b.goods_id
	left outer join zybiro.bi_chili_cat_group d on c.cate_level1_name=d.cat_level1_name
	where
	from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >=regexp_replace(to_date(date_sub(now(),30)),'-','')
	and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') <=regexp_replace(to_date(date_sub(now(),1)),'-','')
	and a.site_id  in(400,600,700,601,900)
	and a.pay_status in(1,3)
	and d.category_group_1 in ('母婴童')
	group by 1
) tt3 on tt2.category_group_1=tt3.category_group_1;
"""

association_sql = """
select
    orders.order_id,
    goods.goods_id
from dw.dw_order_fact as orders
left join dw.dw_order_goods_fact as goods
on orders.order_id = goods.order_id
left join dim.dim_goods c
on c.goods_id=goods.goods_id
left join zybiro.bi_chili_cat_group as p3
on c.cate_level1_name=p3.cat_level1_name
where from_timestamp(case when orders.pay_id=41 then orders.pay_time else orders.result_pay_time end,'yyyyMMdd') >='{begin}'
and from_timestamp(case when orders.pay_id=41 then orders.pay_time else orders.result_pay_time end,'yyyyMMdd') <='{end}'
and orders.site_id  in(400,600,700,601,900) 
and orders.pay_status in(1,3)
and p3.category_group_1 = '母婴童';
""".format(begin=half_year_ago, end=yestoday)

goods_values = """
select
    c.goods_id,
    c.goods_name,
    c.goods_name_zh_cn,
    c.cate_level1_name,
    c.cate_level2_name,
    c.cate_level3_name,
    d.sku_value,
    e.pattern_value
from dim.dim_goods c
left join zybiro.bi_chili_cat_group as p3
on c.cate_level1_name=p3.cat_level1_name
left join ods.ods_who_inventory_sku_relation d
on c.goods_id = d.goods_id
left join dim.dim_goods_pattern e
on c.goods_id = e.goods_id
where c.cate_level1_name = "Kid's Shoes"
and (d.sku_value like '%23%' or d.sku_value like '%24%' or d.sku_value like '%25%' 
    or d.sku_value like '%26%' or d.sku_value like '%27%' or d.sku_value like '%28%'
    or d.sku_value like '%29%' or d.sku_value like '%30%')
and e.pattern_value = 'Girl';
"""

def user_loss_reslae_sql(begin: str, end: str):
    user_loss_reslae_sql = """
    select
        aa.order_id,
        aa.data_date,
        aa.category_group,
        aa.cate_level1_name,
        aa.user_id,
        aa.num,
        aa.revenue
    from (
        select
            a.order_id,
            from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyy-MM-dd') data_date,
            c.category_group,
            c.cate_level1_name,
            a.user_id,
            sum(b.original_goods_number) num,
            sum(b.original_goods_number*b.goods_price) revenue
        from dw.dw_order_goods_fact b 
        inner join  dw.dw_order_fact a on a.order_id=b.order_id
        left join dim.dim_goods c on c.goods_id=b.goods_id
        left join jolly.who_esoloo_supplier  e on  c.provider_code = e.code
        where
        from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >='{begin}'
        and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') <='{end}'
        and a.site_id  in(400,600,700,601,900) 
        and a.pay_status in(1,3)
        and c.category_group in ('家居','母婴','童玩')----='家居'----in ('家居','母婴','童玩')  ---='家居' --
        and c.supplier_genre<>11
        group by
        1,2,3,4,5
        union
        select
            a.order_id,
            from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyy-MM-dd') data_date,
            c.category_group,
            c.cate_level1_name,
            a.user_id,
            sum(b.original_goods_number) num,
            sum(b.original_goods_number*b.goods_price) revenue
        from dw.dw_order_goods_fact b 
        inner join  dw.dw_order_fact a on a.order_id=b.order_id
        left join dim.dim_goods c on c.goods_id=b.goods_id
        left join jolly.who_esoloo_supplier  e on  c.provider_code = e.code
        where
        from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >='{begin}'
        and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') <='{end}'
        and a.site_id  in(400,600,700,601,900) 
        and a.pay_status in(1,3)
        and c.category_group = 'beauty'----='家居'----in ('家居','母婴','童玩')  ---='家居' --
        and c.supplier_genre<>11
        group by
        1,2,3,4,5
    ) as aa
    order by aa.data_date
    """.format(begin=begin, end=end)
    return user_loss_reslae_sql


goods_sale_level = """
select
    aa.year_month,
    aa.category_group,
    aa.cate_level1_name,
    aa.cate_level2_name,
    aa.cate_level3_name,
    aa.goods_id,
    bb.revenue,
    bb.num
from(
    select
        substr(dge.data_date,1,6) as year_month,
        dg.category_group,
        dg.cate_level1_name,
        dg.cate_level2_name,
        dg.cate_level3_name,
        dg.goods_id
    from dim.dim_goods as dg
    left join dim.dim_goods_extend as dge
    on dg.goods_id = dge.goods_id
    and ((cast(dge.data_date as int) >= 20180801
        and cast(dge.data_date as int) <= 20181130)
        or cast(dge.data_date as int) = 20190901)
    where dge.is_jc_on_sale = 1
    and dge.is_jc_sale = 1
    and dg.category_group in ('母婴','童玩')
    and dg.supplier_genre<>11
    group by 1,2,3,4,5,6
) aa
left join(
    select
        from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMM') data_date,
        b.goods_id,
        sum(b.original_goods_number) num,
        sum(b.original_goods_number*b.goods_price) revenue
    from dw.dw_order_goods_fact b 
    inner join  dw.dw_order_fact a on a.order_id=b.order_id
    left join dim.dim_goods c on c.goods_id=b.goods_id
    left join jolly.who_esoloo_supplier  e on  c.provider_code = e.code
    where
    from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >='20180801'
    and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') <='20181130'
    
    and a.site_id  in(400,600,700,601,900) 
    and a.pay_status in(1,3)
    and c.category_group in ('母婴','童玩')----='家居'----in ('家居','母婴','童玩')  ---='家居' --
    and c.supplier_genre<>11
    group by
    1,2
    union
    select
        from_timestamp(add_months(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,1),'yyyyMM') data_date,
        b.goods_id,
        sum(b.original_goods_number) num,
        sum(b.original_goods_number*b.goods_price) revenue
    from dw.dw_order_goods_fact b 
    inner join  dw.dw_order_fact a on a.order_id=b.order_id
    left join dim.dim_goods c on c.goods_id=b.goods_id
    left join jolly.who_esoloo_supplier  e on  c.provider_code = e.code
    where
    from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >='20190801'
    and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') <='20190831'
    
    and a.site_id  in(400,600,700,601,900) 
    and a.pay_status in(1,3)
    and c.category_group in ('母婴','童玩')----='家居'----in ('家居','母婴','童玩')  ---='家居' --
    and c.supplier_genre<>11
    group by
    1,2
) bb
on aa.goods_id = bb.goods_id
and aa.year_month = bb.data_date
"""


order_content = """
select
    from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') data_date,
    a.order_id,
    c.category_group,
    c.cate_level1_name,
    b.goods_id,
    b.goods_name,
    b.original_goods_number,
    b.goods_price,
    b.original_goods_number*b.goods_price revenue,
    case when c.supplier_genre = 11 then 1 else 0 end as is_POP,
    ods_msg.star,
    ods_msg.language,
    ods_msg.content
from dw.dw_order_goods_fact b 
inner join  dw.dw_order_fact a on a.order_id=b.order_id
left join dim.dim_goods c on c.goods_id=b.goods_id
left join jolly_split_order_user.who_goods_comment_message as ods_msg
on a.order_id = ods_msg.order_id
and b.goods_id = ods_msg.goods_id
where
from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >='20190501'
and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') <='20191117'
and a.site_id  in(400,600,700,601,900) 
and a.pay_status in(1,3)
and ods_msg.star = 1
and ods_msg.content is not null
and ods_msg.content != ''
and c.cate_level1_name = "Kid's Shoes"
group by 1,2,3,4,5,6,7,8,9,10,11,12,13
"""

dau_sql = """
select
    dg.category_group,
    substr(gdau.data_date,1,6) as year_month,
    count(distinct(case when gdau.is_imp = 1 then gdau.cookie_id else null end)) as imp_uv,
    count(distinct(case when gdau.is_visit = 1 then gdau.cookie_id else null end)) as visit_uv,
    count(distinct(case when gdau.is_cart = 1 then gdau.cookie_id else null end)) as cart_uv,
    count(distinct(case when gdau.is_order = 1 then gdau.cookie_id else null end)) as orders_uv,
    count(distinct(case when gdau.is_paid = 1 then gdau.cookie_id else null end)) as paid_uv,
    count(distinct(case when gdau.is_fav = 1 then gdau.cookie_id else null end)) as fav_uv,
    count(distinct(case when gdau.is_share = 1 then gdau.cookie_id else null end)) as share_uv
from dw.dw_cookie_dau_goods_relation as gdau
left join dim.dim_goods as dg
on gdau.goods_id = dg.goods_id
where dg.category_group in ('母婴', '童玩', '家居', 'beauty')
and gdau.data_date >= '20190101'
and gdau.data_date < '20190201'
group by 1,2
"""


margin_warning = """
with sales as (
    select
        data_date,
        category_group,
        cate_level1_name,
        provider_code,
        supp_name,
        goods_id,
        goods_name,
        goods_type,
        num,
        revenue,
        cost_with_vat
    from(
        select
            from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') data_date,
            c.category_group,
            c.cate_level1_name,
            c.goods_id,
            c.goods_name,
            c.provider_code,
            sun.supp_name,
            case when b.is_gift=5 then '闪购'
                when b.goods_price=0 then '免费礼品'
                when b.is_gift=0 and wod.is_main=0 then '原价'
                when b.is_gift=0 and wod.is_main=1 then '人工定价'
                when b.is_gift=0 and wod.is_main=2 then '新人价'
                when b.is_gift=0 and wod.is_main=3 then '会员价'
                when b.is_gift=0 and wod.is_main in (5,6) then '智能定价'
                else null end as goods_type,
            sum(b.original_goods_number) num,
            sum(b.original_goods_number*b.goods_price) revenue,
            sum(b.original_goods_number*b.in_price_usd) +sum( case when ((lower(a.country_name)='saudi arabia' and e.is_sa_supplier<>1) 
                or  (lower(a.country_name)='united arab emirates' and e.supplier_genre<>10)) then b.goods_price*b.original_goods_number*0.05
                when lower(a.country_name) in ('saudi arabia','united arab emirates')
                then  (b.goods_price-b.in_price_usd)*b.original_goods_number/1.05*0.05 end) cost_with_vat
        from dw.dw_order_goods_fact b 
        inner join  dw.dw_order_fact a on a.order_id=b.order_id
        left join dim.dim_goods c on c.goods_id=b.goods_id
        left join zybiro.bi_chili_cat_group as cat
        on c.cate_level1_name = cat.cat_level1_name
        left join jolly.who_esoloo_supplier  e on  c.provider_code = e.code
        left join jolly.who_order_goods as wod
        on b.order_id = wod.order_id
        and b.goods_id = wod.goods_id
        and b.rec_id = wod.rec_id
        left join zybiro.bi_elaine_supp_name as sun
        on c.provider_code = sun.code
        where
        from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') =from_timestamp(date_sub(now(),1), 'yyyyMMdd')
        and a.site_id  in(400,600,700,601,900) 
        and a.pay_status in(1,3)
        and cat.business_group = '家居&美妆'
        and c.supplier_genre<>11
        group by 1,2,3,4,5,6,7,8
    ) as base
),

pri_in_come as (
   select
        a.goods_id,
        sum(profits2) pre_income
    from zybiro.bi_faye_net_profit_precast_new a
    left join dim.dim_goods g on a.goods_id=g.goods_id
    left join zybiro.bi_chili_cat_group p3
    on g.cate_level1_name=p3.cat_level1_name
    where data_date=from_timestamp(date_sub(now(),1), 'yyyyMMdd')
    and regexp_replace(pay_date,'-','') > from_timestamp(date_sub(now(),2), 'yyyyMMdd')
    and p3.business_group = '家居&美妆'
    group by 1
)

select
    data_date as `日期`,
    category_group as `类目`,
    cate_level1_name as `一级`,
    provider_code as `供应商编码`,
    supp_name as `供应商名称`,
    goods_id,
    goods_name as `商品名称`,
    goods_type as `定价类型`,
    num as `销量`,
    round(revenue,2) as `GMV`,
    round(cost_with_vat,2) as `成本`,
    case when revenue=0 or cost_with_vat=0 then null 
        else concat(cast(dround(margin*100,2) as string),'%')
        end as `毛利率`
from(
    select
        data_date,
        category_group,
        cate_level1_name,
        sales.provider_code,
        sales.supp_name,
        sales.goods_id,
        sales.goods_name,
        sales.goods_type,
        sum(num) as num,
        sum(revenue) as revenue,
        sum(cost_with_vat) as cost_with_vat,
        (sum(revenue)-sum(cost_with_vat))/sum(revenue) as margin,
        sum(pre_income) as pre_income
    from sales left join pri_in_come
    on sales.goods_id = pri_in_come.goods_id
    group by 1,2,3,4,5,6,7,8
) as base
where margin <0.52
order by goods_id
"""


sku_sql = """
select
    base.*,
    sales.num,
    sales.revenue,
    sales.cost_with_vat,
    label.lable_value_name
from(
    select
        dg.cate_level1_name,
        dg.cate_level2_name,
        dg.cate_level3_name,
        dg.goods_name,
        dg.goods_link,
        dg.goods_id,
        --- sku.rec_id,
        dg.first_on_sale_time,
        avg(dge.in_price) as in_price,
        avg(dge.prst_price_1) as prst_price_1
    from dim.dim_goods as dg
    left join dim.dim_goods_extend as dge
    on dg.goods_id = dge.goods_id
    and dge.data_date = '20190930'
    left join jolly.who_sku_relation as sku
    on dg.goods_id = sku.goods_id
    where dg.cate_level1_name in ("Kid's Clothing", "Kid's Bags", "Kid's Accessories", "Kid's Shoes")
    and dg.supplier_genre <> 11
    and dge.is_jc_on_sale = 1
    and dge.is_jc_sale = 1
    group by 1,2,3,4,5,6,7,8
) as base
left join(
    select
        b.goods_id,
        --- b.sku_id,
        avg(b.goods_price) as goods_price,
        avg(b.in_price) as in_price,
        sum(b.original_goods_number) num,
        sum(b.original_goods_number*b.goods_price) revenue,
        sum(b.original_goods_number*b.in_price_usd) +sum( case when ((lower(a.country_name)='saudi arabia' and e.is_sa_supplier<>1) 
            or  (lower(a.country_name)='united arab emirates' and e.supplier_genre<>10)) then b.goods_price*b.original_goods_number*0.05
            when lower(a.country_name) in ('saudi arabia','united arab emirates')
            then  (b.goods_price-b.in_price_usd)*b.original_goods_number/1.05*0.05 end) cost_with_vat
    from dw.dw_order_goods_fact b 
    inner join  dw.dw_order_fact a on a.order_id=b.order_id
    left join dim.dim_goods c on c.goods_id=b.goods_id
    left join jolly.who_esoloo_supplier  e on  c.provider_code = e.code
    where
    from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >='20190901'
    and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') <='20190930'
    and a.site_id  in(400,600,700,601,900) 
    and a.pay_status in(1,3)
    and c.cate_level1_name in ("Kid's Clothing", "Kid's Bags", "Kid's Accessories", "Kid's Shoes")
    and c.supplier_genre<>11
    group by 1,2
) as sales
on base.goods_id = sales.goods_id
left join(
    select 
        a.goods_id,
        case when label_value_id=945 then '男大童'
            when label_value_id=947 then '男小童'
            when label_value_id=949 then '女大童'
            when label_value_id=951 then '女小童'
            when label_value_id=953 then '大童'
            when label_value_id=955 then '小童'
            when label_value_id=957 then '婴儿' end as lable_value_name
    from jolly.who_goods_label_relation a 
    inner join dim.dim_jc_goods b on a.goods_id=b.goods_id
    and b.cat_level1_name in ("Kid's Clothing", "Kid's Bags", "Kid's Accessories", "Kid's Shoes")
    where label_key_id=93
) as label
on base.goods_id = label.goods_id;
"""


stock_msg = """
select
    base.data_date,
    base.order_id,
    base.goods_id,
    base.num as `销售数量`,
    base.revenue as `销售额`,
    stock.overseas_num as `海外仓数量`,
    stock.num as `国内仓数量`
from(
    select
        a.order_id,
        from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') as data_date,
        b.goods_id,
        sum(b.original_goods_number) as num,
        sum(b.original_goods_number*b.goods_price) revenue
    from dw.dw_order_goods_fact b 
    inner join  dw.dw_order_fact a on a.order_id=b.order_id
    left join dim.dim_goods c on c.goods_id=b.goods_id
    left join jolly.who_esoloo_supplier  e on  c.provider_code = e.code
    where
    from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >='20190801'
    and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') <='201910831'
    and a.site_id  in(400,600,700,601,900) 
    and a.pay_status in(1,3)
    and c.category_group in ('家居','母婴','童玩','beauty')----='家居'----in ('家居','母婴','童玩')  ---='家居' --
    and c.supplier_genre<>11
    group by 1,2,3
) as base
left join(
    select
        base.goods_id,
        base.data_date,
        sum(case when is_pop=1 then base.free_stock else 0 end) as overseas_num,
        sum(case when is_pop=0 then base.free_stock else 0 end) as num
    from(
        select
            stock.goods_id,
            stock.rec_id,
            stock.data_date,
            case when stock.depot_id in (15,16) then 1 else 0 end as is_pop,
            total_stock_num - total_order_lock_num - total_return_lock_num - total_allocate_lock_num as free_stock
        from ods.ods_who_wms_goods_stock_total_detail as stock
        left join dim.dim_goods as dg
        on stock.goods_id = dg.goods_id
        where dg.category_group in ('家居','母婴','童玩','beauty')
    ) as base
    group by 1,2
) as stock
on base.goods_id=stock.goods_id
and cast(base.data_date as int) = cast(stock.data_date as int)
order by order_id
"""


supplier_price = """
with base as (
    select
        cat.category_group_1,
        case when dg.cate_level1_name='Beauty' then dg.cate_level2_name else dg.cate_level1_name end as cate_level1_name,
        dg.goods_id,
        sku.rec_id,
        dg.provider_code,
        pm.in_price_max,
        sku.in_price
    from dim.dim_goods as dg
    left join dim.dim_goods_extend as dge
    on dg.goods_id = dge.goods_id
    left join ods.ods_who_sku_relation as sku
    on dg.goods_id = sku.goods_id
    and dge.data_date = sku.data_date
    left join zybiro.bi_chili_cat_group as cat
    on dg.cate_level1_name = cat.cat_level1_name
    left join(
        select
            dg.goods_id,
            sku.rec_id,
            max(sku.in_price) as in_price_max
        from dim.dim_goods as dg
        left join dim.dim_goods_extend as dge
        on dg.goods_id = dge.goods_id
        left join ods.ods_who_sku_relation as sku
        on dg.goods_id = sku.goods_id
        and dge.data_date = sku.data_date
        left join zybiro.bi_chili_cat_group as cat
        on dg.cate_level1_name = cat.cat_level1_name
        where dg.category_group in ('家居','母婴','童玩', 'beauty')
        and dge.data_date > '20190201'
        and dg.supplier_genre <> 11
        and dge.is_jc_on_sale = 1
        and dge.is_jc_sale = 1
        group by 1,2
    ) as pm
    on dg.goods_id = pm.goods_id
    and sku.rec_id = pm.rec_id
    where dg.category_group in ('家居','母婴','童玩', 'beauty')
    and dge.data_date = from_timestamp(date_sub(now(),1), 'yyyyMMdd')
    and dg.first_on_sale_time < '2019-10-28'
    and dg.supplier_genre <> 11
    and dge.is_jc_on_sale = 1
    and dge.is_jc_sale = 1
),

act as (
    select 
        a.activity_id,a.goods_id,a.sku_id,a.status
        ,a.original_in_price,a.activity_price,a.currency
        ,b.activity_name
        ,from_unixtime(b.start_time, "yyyyMMdd")start_time
        ,from_unixtime(b.end_time, "yyyyMMdd")end_time
        ,c.supp_code
        ,c.supp_name
    from jolly.who_supplier_activity_goods_details a
    left join jolly.who_supplier_activity b on a.activity_id=b.activity_id
    inner join jolly.who_supplier_activity_goods c on a.activity_id=c.activity_id and a.activity_goods_id=c.rec_id and c.status <>-1
    where b.activity_id in (443,434,573,575,577,579,589,591,595,597,599)
    group by 1,2,3,4,5,6,7,8,9,10,11,12
),

sales as (
    select
        case when c.cate_level1_name="Beauty" then c.cate_level2_name else c.cate_level1_name end as cate_level1_name,
        b.goods_id,
        b.sku_id,
        sum(b.original_goods_number) num
    from dw.dw_order_goods_fact b 
    inner join  dw.dw_order_fact a on a.order_id=b.order_id
    left join dim.dim_goods c on c.goods_id=b.goods_id
    left join jolly.who_esoloo_supplier  e on  c.provider_code = e.code
    where
    from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >='20191001'
    and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') <='20191031'
    and a.site_id  in(400,600,700,601,900) 
    and a.pay_status in(1,3)
    and c.category_group in ('家居','母婴','童玩', 'beauty')
    and c.supplier_genre<>11
    group by 1,2,3
),

act_msg as (
    select
        category_group_1,
        cate_level1_name,
        provider_code,
        goods_id,
        sku,
        count(distinct case when is_act = 1 then goods_id else null end) as is_acts,
        max(original_in_price) as original_in_price,
        min(activity_price) as activity_price
    from(
        select
            base0.category_group_1,
            base0.cate_level1_name,
            base0.goods_id,
            base0.rec_id as sku,
            base0.in_price_max,
            base0.in_price,
            base0.provider_code,
            base0.is_act,
            case when base0.original_in_price is null then base0.in_price_max
                when base0.in_price_max > base0.original_in_price then base0.in_price_max
                else base0.original_in_price end as original_in_price,
            case when base0.activity_price is null then base0.in_price
                when base0.in_price < base0.activity_price then base0.in_price
                else base0.activity_price end as activity_price
        from (
            select
                base.category_group_1,
                base.cate_level1_name,
                base.goods_id,
                base.rec_id,
                base.provider_code,
                case when act.activity_id is null then 0 else 1 end as is_act,
                max(base.in_price_max) as in_price_max,
                min(base.in_price) as in_price,
                max(act.original_in_price) as original_in_price,
                min(act.activity_price) as activity_price
            from base left join act
            on base.goods_id = act.goods_id
            and base.rec_id = act.sku_id
            group by 1,2,3,4,5,6
        ) as base0
    ) as base
    group by 1,2,3,4,5
)

select
    category_group_1
    ,cate_level1_name
    ,base.provider_code                                                                          as `供应商编码`
    ,count(distinct case when base.is_acts = 1 then base.goods_id else null end)                 as `活动商品数`
    ,sum(base.original_in_price*base.num)                                                        as `原成本`
    ,sum(base.activity_price*base.num)                                                           as `活动成本`
    ,(sum(base.original_in_price*num) - sum(activity_price*num))/sum(base.original_in_price*num) as `降价幅度`
from(
    select
        act_msg.category_group_1,
        act_msg.cate_level1_name,
        act_msg.provider_code,
        act_msg.goods_id,
        act_msg.sku,
        act_msg.is_acts,
        act_msg.original_in_price,
        act_msg.activity_price,
        case when sales.num is null then 1 else sales.num end as num
    from act_msg left join sales
    on act_msg.goods_id = sales.goods_id
    and act_msg.sku = sales.sku_id
) as base
group by 1,2,3
"""


zhouquannan="""
select
    c.category_group,
    c.goods_id,
    c.goods_name,
    brand.brand_name,
    days0.on_sale_days,
    case when c.cate_level1_name="Beauty" then c.cate_level2_name else c.cate_level1_name end as cate_level1_name,
    sum(b.original_goods_number) num,
    sum(b.original_goods_number*b.goods_price) revenue,
    sum(b.original_goods_number*b.in_price_usd) +nvl(sum( case when ((lower(a.country_name)='saudi arabia' and e.is_sa_supplier<>1) 
        or  (lower(a.country_name)='united arab emirates' and e.supplier_genre<>10)) then b.goods_price*b.original_goods_number*0.05
        when lower(a.country_name) in ('saudi arabia','united arab emirates')
        then  (b.goods_price-b.in_price_usd)*b.original_goods_number/1.05*0.05 end),0) cost_with_vat
from dw.dw_order_goods_fact b 
inner join  dw.dw_order_fact a on a.order_id=b.order_id
left join dim.dim_goods c on c.goods_id=b.goods_id
left join jolly.who_esoloo_supplier  e on  c.provider_code = e.code
left join jolly.who_brand as brand on c.brand_id = brand.brand_id
left join (
    select
        dg.goods_id,
        max(dge.on_sale_days) as on_sale_days
    from dim.dim_goods as dg 
    left join dim.dim_goods_extend as dge
    on dg.goods_id = dge.goods_id
    where dg.category_group = 'beauty'
    and dg.supplier_genre=5
    group by 1
) as days0
on c.goods_id = days0.goods_id
where
a.site_id  in(400,600,700,601,900) 
and a.pay_status in(1,3)
and c.supplier_genre<>11
and c.category_group = 'beauty'
and c.supplier_genre=5
group by 1,2,3,4,5,6;
"""


def act_hour():
    drop_sql = """
    drop table zybiro.bi_longjy_act_hour;
    """
    act_hour = """
    create table zybiro.bi_longjy_act_hour as
    select
        from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end, 'yyyy-MM-dd') as data_date,
        from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end, 'dd') as days0,
        cast(from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end, 'HH') as int) as time_hour,
        cat.category_group,
        case when c.cate_level1_name='Beauty' then c.cate_level2_name else c.cate_level1_name end as cate_level1_name,
        sum(b.original_goods_number) num,
        sum(b.original_goods_number*b.goods_price) revenue,
        sum(b.original_goods_number*b.in_price_usd) +nvl(sum( case when ((lower(a.country_name)='saudi arabia' and e.is_sa_supplier<>1) 
            or  (lower(a.country_name)='united arab emirates' and e.supplier_genre<>10)) then b.goods_price*b.original_goods_number*0.05
            when lower(a.country_name) in ('saudi arabia','united arab emirates')
            then  (b.goods_price-b.in_price_usd)*b.original_goods_number/1.05*0.05 end),0) cost_with_vat
    from dw.dw_order_goods_fact b 
    inner join  dw.dw_order_fact a on a.order_id=b.order_id
    left join dim.dim_goods c on c.goods_id=b.goods_id
    left join jolly.who_esoloo_supplier  e on  c.provider_code = e.code
    left join dim.dim_goods_category_group_new as cat
    on c.cate_level1_name = cat.cate_level1_name
    where
    from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >= from_timestamp(trunc(now(),'MM'),'yyyyMMdd')
    and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') <= from_timestamp(date_sub(now(),1),'yyyyMMdd')
    and a.site_id  in(400,600,700,601,900) 
    and a.pay_status in(1,3)
    and cat.category_group in ('家居','母婴','童玩','beauty','婴童时尚','孕婴童用品')
    and c.supplier_genre<>11
    group by 1,2,3,4,5
    union
    select
        from_timestamp(case when od.pay_id=41 then od.pay_time else od.result_pay_time end, 'yyyy-MM-dd') as data_date,
        from_timestamp(case when od.pay_id=41 then od.pay_time else od.result_pay_time end, 'dd') as days0,
        cast(from_timestamp(case when od.pay_id=41 then od.pay_time else od.result_pay_time end, 'HH') as int) as time_hour,
        cat.category_group,
        case when dg.cate_level1_name='Beauty' then dg.cate_level2_name else dg.cate_level1_name end as cate_level1_name,
        sum(odf.original_goods_number) num,
        sum(odf.original_goods_number*odf.goods_price) revenue,
        sum(odf.original_goods_number*odf.in_price_usd) +nvl(sum( case when ((lower(od.country_name)='saudi arabia' and e.is_sa_supplier<>1) 
            or  (lower(od.country_name)='united arab emirates' and e.supplier_genre<>10)) then odf.goods_price*odf.original_goods_number*0.05
            when lower(od.country_name) in ('saudi arabia','united arab emirates')
            then  (odf.goods_price-odf.in_price_usd)*odf.original_goods_number/1.05*0.05 end),0) cost_with_vat
    from rt.dw_s_order_fact as od
    left join rt.dw_s_order_goods_fact as odf
    on od.order_id=odf.order_id
    left join dim.dim_goods as dg
    on odf.goods_id = dg.goods_id
    left join jolly.who_esoloo_supplier e
    on dg.provider_code = e.code
    left join dim.dim_goods_category_group_new as cat
    on dg.cate_level1_name = cat.cate_level1_name
    where from_timestamp(case when od.pay_id=41 then od.pay_time else od.result_pay_time end,'yyyyMMdd') = from_timestamp(to_date(now()), "yyyyMMdd")
    and od.site_id  in(400,600,700,601,900)
    and od.pay_status in(1,3)
    and cat.category_group in ('家居','母婴','童玩','beauty','婴童时尚','孕婴童用品')
    and dg.supplier_genre<>11
    group by 1,2,3,4,5
    """
    return drop_sql, act_hour


def bu_daily_report_sql():
    drop_sql = """
    drop table zybiro.bi_longjy_daily_report_bu_day_sale_margin;
    """

    create_sql = """
    create table zybiro.bi_longjy_daily_report_bu_day_sale_margin as
    select
        case when base.category_group in ('家居','婚庆周边')                                             then '家居'
            when base.category_group in ('婴童时尚','孕婴童用品')                                        then '母婴童'
            when base.category_group in ('女装','服装2（内衣）','鞋包','ACC','男装',"包", "鞋",'Sports') then '时尚'
            when base.category_group in ('数码', '车品','Appliances','Computers & Accessories')          then '3C'
            when base.category_group in ('手机')                                                         then '手机'
            when base.category_group in ('beauty')                                                       then '美妆' 
            when base.category_group in ('快消')                                                         then '执御超市'
            else '其他' end                                                                              as `BU`
        ,cate_level1_name
        ,data_date
        ,is_pop
        ,substr(data_date,1,4) as years0
        ,round(sum(revenue),2)                                                                as revenue
        ,round(sum(cost_with_vat),2)                                                          as cost_with_vat
        ,(sum(revenue) - sum(cost_with_vat))/sum(revenue)                                     as margin
        ,sum(base.expo_uv)                                                                    as expo_uv
        ,sum(base.expo_pv)                                                                    as expo_pv
        ,sum(base.goods_click_uv)                                                             as goods_click_uv
        ,sum(base.bill_uv)                                                                    as bill_uv
        ,sum(base.sales_uv)                                                                   as sales_uv
    from (
            select
            sales.category_group,
            sales.cate_level1_name,
            sales.data_date,
            sales.is_pop,
            sales.num,
            sales.revenue,
            sales.cost_with_vat,
            dau.expo_pv,
            dau.expo_uv,
            dau.goods_click_uv,
            dau.bill_uv,
            dau.sales_uv
        from (
            select
                from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') data_date,
                cat.category_group,
                case when c.cate_level1_name="Beauty" then c.cate_level2_name else c.cate_level1_name end as cate_level1_name,
                case when c.supplier_genre = 11 then 1 else 0 end as is_pop,
                sum(b.original_goods_number) num,
                sum(b.original_goods_number*b.goods_price) revenue,
                sum(b.original_goods_number*b.in_price_usd) +nvl(sum( case when ((lower(a.country_name)='saudi arabia' and e.is_sa_supplier<>1) 
                    or  (lower(a.country_name)='united arab emirates' and e.supplier_genre<>10)) then b.goods_price*b.original_goods_number*0.05
                    when lower(a.country_name) in ('saudi arabia','united arab emirates')
                    then  (b.goods_price-b.in_price_usd)*b.original_goods_number/1.05*0.05 end),0) cost_with_vat
            from dw.dw_order_goods_fact b 
            inner join  dw.dw_order_fact a on a.order_id=b.order_id
            left join dim.dim_goods c on c.goods_id=b.goods_id
            left join jolly.who_esoloo_supplier  e on  c.provider_code = e.code
            left join dim.dim_goods_category_group_new as cat
            on c.cate_level1_name = cat.cate_level1_name
            where
            ((from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >= from_timestamp(trunc(months_sub(date_sub(now(),1),2), "MM"), "yyyyMMdd")
                and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') <= from_timestamp(date_sub(now(),1), "yyyyMMdd") )
            or (from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >= from_timestamp(years_sub(trunc(months_sub(date_sub(now(),1),2), "MM"), 1), "yyyyMMdd")
                and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd')<= from_timestamp(years_sub(date_sub(now(),1), 1), "yyyyMMdd")  ))
            and a.site_id  in(400,600,700,601,900) 
            and a.pay_status in(1,3)
            group by 1,2,3,4
        ) as sales 
        left join (
                select
                    cat.category_group,
                    case when dg.cate_level1_name="Beauty" then dg.cate_level2_name else dg.cate_level1_name end as cate_level1_name,
                    dau.data_date,
                    case when dg.supplier_genre = 11 then 1 else 0 end as is_POP,
                    sum(dau.expo_pv) as expo_pv,
                    sum(dau.expo_uv) as expo_uv,
                    sum(dau.goods_click_uv) as goods_click_uv,
                    sum(dau.bill_uv) as bill_uv,
                    sum(dau.bill_uv)/sum(dau.goods_click_uv) as click_bill,
                    sum(dau.sales_uv) as sales_uv,
                    sum(dau.sales_uv)/sum(dau.goods_click_uv) as click_sale
                from rpt.rpt_sum_goods_daily as dau
                left join dim.dim_goods as dg
                on dau.goods_id = dg.goods_id
                left join dim.dim_goods_category_group_new as cat
                on dg.cate_level1_name = cat.cate_level1_name
                where dau.data_date >= from_timestamp(trunc(months_sub(date_sub(now(),1),2), "MM"), "yyyyMMdd")
                and dau.data_date <= from_timestamp(date_sub(now(),1), "yyyyMMdd")
                and dau.site_id in (400,600,700,601,900)
                group by 1,2,3,4
        ) as dau
        on sales.category_group = dau.category_group
        and sales.cate_level1_name = dau.cate_level1_name
        and sales.data_date = dau.data_date
        and sales.is_pop = dau.is_pop
    ) as base
    where ((data_date >= from_timestamp(trunc(months_sub(date_sub(now(),1),2), "MM"), "yyyyMMdd")
      and data_date <= from_timestamp(date_sub(now(),1), "yyyyMMdd"))
    or (data_date >= from_timestamp(years_sub(trunc(months_sub(date_sub(now(),1),2), "MM"), 1), "yyyyMMdd")
      and data_date <= from_timestamp(years_sub(date_sub(now(),1), 1), "yyyyMMdd")))
    and base.category_group != '快消'
    group by 1,2,3,4
    order by case when base.category_group in ('家居','婚庆周边')                                        then '家居'
            when base.category_group in ('婴童时尚','孕婴童用品')                                        then '母婴童'
            when base.category_group in ('女装','服装2（内衣）','鞋包','ACC','男装',"包", "鞋",'Sports') then '时尚'
            when base.category_group in ('数码', '车品','Appliances','Computers & Accessories')          then '3C'
            when base.category_group in ('手机')                                                         then '手机'
            when base.category_group in ('beauty')                                                       then '美妆' 
            when base.category_group in ('快消')                                                         then '执御超市'
            else '其他' end
        ,cate_level1_name
        ,data_date
    """
    return drop_sql, create_sql


act_gmv_hour_0 = """
    select
        cat.category_group,
        case when dg.cate_level1_name="Beauty" then dg.cate_level2_name else dg.cate_level1_name end as cate_level1_name,
        from_timestamp(case when od.pay_id=41 then od.pay_time else od.result_pay_time end, 'HH') as time_hour,
        from_timestamp(case when od.pay_id=41 then od.pay_time else od.result_pay_time end, 'dd') as days0,
        count(distinct od.order_id) as order_num,
        sum(odf.original_goods_number) num,
        sum(odf.original_goods_number*odf.goods_price) revenue
    from rt.dw_s_order_fact as od
    left join rt.dw_s_order_goods_fact as odf
    on od.order_id=odf.order_id
    left join dim.dim_goods as dg
    on odf.goods_id = dg.goods_id
    left join jolly.who_esoloo_supplier e
    on dg.provider_code = e.code
    left join dim.dim_goods_category_group_new as cat
    on dg.cate_level1_name = cat.cate_level1_name
    where (from_timestamp(case when od.pay_id=41 then od.pay_time else od.result_pay_time end,'yyyyMMdd') = from_timestamp(now(), "yyyyMMdd")
    or from_timestamp(case when od.pay_id=41 then od.pay_time else od.result_pay_time end,'yyyyMMdd') = from_timestamp(date_sub(now(),7), "yyyyMMdd"))
    and od.site_id  in(400,600,700,601,900)
    and od.pay_status in(1,3)
    and cat.category_group in ('家居','母婴','童玩','beauty', '婴童时尚', '孕婴童用品')
    and dg.supplier_genre<>11
    group by 1,2,3,4
"""

act_gmv_hour_1 = """
select
    goods.category_group,
    goods.cate_level1_name,
    goods.data_date,
    sales.time_hour,
    goods.goods_id,
    goods.goods_name,
    sales.order_num,
    sales.num,
    sales.revenue
from (
select
        cat.category_group,
        case when dg.cate_level1_name='Beauty' then dg.cate_level2_name else dg.cate_level1_name end as cate_level1_name,
        dg.goods_id,
        dg.goods_name,
        dge.data_date
    from dim.dim_goods as dg
    left join dim.dim_goods_extend as dge
    on dg.goods_id = dge.goods_id
    and dge.data_date = from_timestamp(date_sub(now(),1), "yyyyMMdd")
    left join dim.dim_goods_category_group_new as cat
    on dg.cate_level1_name = cat.cate_level1_name
    where dg.supplier_genre <> 11
    and cat.category_group in ('家居','母婴','童玩','beauty', '婴童时尚', '孕婴童用品')
    and dge.is_jc_on_sale = 1
    and dge.is_jc_sale = 1
) as goods
left join (
    select
        from_timestamp(case when od.pay_id=41 then od.pay_time else od.result_pay_time end, 'HH') as time_hour,
        odf.goods_id,
        count(distinct od.order_id) as order_num,
        sum(odf.original_goods_number) num,
        sum(odf.original_goods_number*odf.goods_price) revenue
    from rt.dw_s_order_fact as od
    left join rt.dw_s_order_goods_fact as odf
    on od.order_id=odf.order_id
    left join dim.dim_goods as dg
    on odf.goods_id = dg.goods_id
    left join jolly.who_esoloo_supplier e
    on dg.provider_code = e.code
    left join dim.dim_goods_category_group_new as cat
    on dg.cate_level1_name = cat.cate_level1_name
    where from_timestamp(case when od.pay_id=41 then od.pay_time else od.result_pay_time end,'yyyyMMdd') = from_timestamp(now(), "yyyyMMdd")
    and od.site_id  in(400,600,700,601,900)
    and od.pay_status in(1,3)
    and cat.category_group in ('家居','母婴','童玩','beauty', '婴童时尚', '孕婴童用品')
    and dg.supplier_genre<>11
    group by 1,2
) as sales
on goods.goods_id = sales.goods_id;
"""


goods_hour_dau = """
select
    dau.category_group,
    dau.cate_level1_name,
    dau.goods_id,
    dau.data_date as `日期`,
    dau.hours0 as `时`,
    dau.dau as `点击uv`,
    dau.visitcounts as `点击pv`,
    addbag.add_uv as `加购uv`,
    addbag.add_pv as `加购pv`,
    addbag.add_goods_num as `加购商品数量`
from(
    select
        cat.category_group,
        case when dg.cate_level1_name="Beauty" then dg.cate_level2_name else dg.cate_level1_name end as cate_level1_name,
        dg.goods_id,
        logs.data_date,
        hour(logs.servertime) as hours0,
        count(distinct logs.cookieid) as dau,
        sum(cast(logs.visitcount as int)) as visitcounts
    from rt.ods_s_page_view_log as logs
    left join dim.dim_goods as dg
    on logs.goodsid = dg.goods_id
    left join dim.dim_goods_category_group_new as cat
    on dg.cate_level1_name=cat.cate_level1_name
    where logs.data_date = from_timestamp(to_date(now()), "yyyyMMdd")
    and logs.siteid in (400,600,700,601,900)
    and cat.category_group in ('家居','beauty', '婴童时尚', '孕婴童用品')
    group by 1,2,3,4,5
) as dau
left join(
    select
        cat.category_group,
        case when dg.cate_level1_name="Beauty" then dg.cate_level2_name else dg.cate_level1_name end as cate_level1_name,
        logs.goodsid,
        logs.data_date,
        hour(logs.servertime) as hours0,
        count(distinct logs.cookieid) as add_uv,
        sum(logs.eventcount) as add_pv,
        sum(logs.goodsnum) as add_goods_num
    from rt.ods_s_event_log as logs
    left join dim.dim_goods as dg
    on logs.goodsid = dg.goods_id
    left join dim.dim_goods_category_group_new as cat
    on dg.cate_level1_name = cat.cate_level1_name
    where data_date = from_timestamp(to_date(now()), "yyyyMMdd")
    and logs.siteid in (400,600,700,601,900)
    and cat.category_group in ('家居','beauty', '婴童时尚', '孕婴童用品')
    and dg.supplier_genre <> 11
    and logs.eventkey = 'key_addtobag_result'
    group by 1,2,3,4,5
) as addbag
on dau.category_group = addbag.category_group
and dau.cate_level1_name = addbag.cate_level1_name
and dau.goods_id = addbag.goodsid
and dau.data_date = addbag.data_date
and dau.hours0 = addbag.hours0
order by dau.category_group,
    dau.cate_level1_name,
    dau.goods_id,
    dau.data_date,
    dau.hours0
"""


hour_dau = """
select
    dau.category_group,
    dau.cate_level1_name,
    dau.data_date,
    dau.hours0 as `时`,
    dau.dau as `点击uv`,
    dau.visitcounts as `点击pv`,
    addbag.add_uv as `加购uv`,
    addbag.add_pv as `加购pv`,
    addbag.add_goods_num as `加购商品数量`
from(
    select
        cat.category_group,
        case when dg.cate_level1_name="Beauty" then dg.cate_level2_name else dg.cate_level1_name end as cate_level1_name,
        logs.data_date,
        hour(logs.servertime) as hours0,
        count(distinct logs.cookieid) as dau,
        sum(cast(logs.visitcount as int)) as visitcounts
    from rt.ods_s_page_view_log as logs
    left join dim.dim_goods as dg
    on logs.goodsid = dg.goods_id
    left join dim.dim_goods_category_group_new as cat
    on dg.cate_level1_name=cat.cate_level1_name
    where logs.data_date = from_timestamp(to_date(now()), "yyyyMMdd")
    and logs.siteid in (400,600,700,601,900)
    and cat.category_group in ('家居','beauty', '婴童时尚', '孕婴童用品')
    and dg.supplier_genre <> 11
    group by 1,2,3,4
) as dau
left join (
    select
        cat.category_group,
        case when dg.cate_level1_name="Beauty" then dg.cate_level2_name else dg.cate_level1_name end as cate_level1_name,
        logs.data_date,
        hour(logs.servertime) as hours0,
        count(distinct logs.cookieid) as add_uv,
        sum(logs.eventcount) as add_pv,
        sum(logs.goodsnum) as add_goods_num
    from rt.ods_s_event_log as logs
    left join dim.dim_goods as dg
    on logs.goodsid = dg.goods_id
    left join dim.dim_goods_category_group_new as cat
    on dg.cate_level1_name = cat.cate_level1_name
    where data_date = from_timestamp(to_date(now()), "yyyyMMdd")
    and logs.siteid in (400,600,700,601,900)
    and cat.category_group in ('家居','beauty', '婴童时尚', '孕婴童用品')
    and dg.supplier_genre <> 11
    and logs.eventkey = 'key_addtobag_result'
    group by 1,2,3,4
) as addbag
on dau.category_group = addbag.category_group
and dau.cate_level1_name = addbag.cate_level1_name
and dau.data_date = addbag.data_date
and dau.hours0 = addbag.hours0
order by dau.category_group,
    dau.cate_level1_name,
    dau.data_date,
    dau.hours0
"""


def kids_week_month_sql(period: str):
    if period == 'week':
        basesqlmsg = """
        select
            goods.category_group,
            goods.cate_level1_name,
            goods.cate_level2_name,
            goods.cate_level3_name,
            goods.cate_level4_name,
            goods.goods_link as `链接`,
            goods.goods_id,
            goods.sku_num as `sku数量`,
            
            rpts.`30d_expo_pv` as `曝光数`,
            rpts.goods_click_uv as `点击数`,
            rpts.goods_click_uv/rpts.`30d_expo_uv` as `点击率`,
            rpts.`30d_sales_uv`/rpts.goods_click_uv as `转化率`,
            sales.num as `销量`,
            sales.revenue as `GMV`,
            sales.num/goods.date_num as `日均销量`,
            case when sales.num/goods.date_num < 1 then '滞销'
                when sales.num/goods.date_num>=1 and sales.num/goods.date_num<3.4 then '平销'
                when sales.num/goods.date_num>=3.4 then '畅销'
                else null end as `畅销类型`,
            goods.in_price as `成本(￥)`,
            goods.in_price_usd as `成本($)`,
            goods.prst_price_1 as `销售价($)`,
            
            stock.free_stock_num as `库存`,
            goods.is_jc_sale as `是否可售`,
            goods.is_jc_on_sale as `是否在架`,
            goods.provider_code as `供应商编码`,
            goods.supp_name as `供应商名称`,
            goods.first_on_sale_time as `上架时间`,
            patterns.material,
            patterns.decoration,
            patterns.color,
            patterns.patterns,
            patterns.collar_type,
            patterns.applicable_gender,
            patterns.applicable_stage,
            patterns.lengths,
            patterns.sleeve_length,
            patterns.sleeve_type
        from (
            select
                cat.category_group,
                dg.cate_level1_name,
                dg.cate_level2_name,
                dg.cate_level3_name,
                dg.cate_level4_name,
                dg.goods_id,
                count(sku.rec_id) as sku_num,
                dgi.main_img_url as goods_link,
                dge.is_jc_sale,
                dge.is_jc_on_sale,
                dg.provider_code,
                supp.supp_name,
                dge.in_price,
                dge.in_price_usd,
                dge.prst_price_1,
                dg.first_on_sale_time,
                onsale.date_num
            from dim.dim_goods as dg
            left join dim.dim_goods_extend as dge
            on dg.goods_id = dge.goods_id
            and dge.data_date = from_timestamp(date_sub(now(),1), "yyyyMMdd")
            left join dim.dim_goods_category_group_new as cat
            on dg.cate_level1_name = cat.cate_level1_name
            left join zybiro.bi_elaine_supp_name as supp
            on dg.provider_code = supp.code
            left join jolly.who_sku_relation as sku
            on dg.goods_id = sku.goods_id
            left join dim.dim_goods_image as dgi
            on cast(dg.goods_id as int) = cast(dgi.goods_id as int)
            left join (
                select
                    dg.goods_id,
                    sum(case when dge.is_jc_on_sale=1 then 1 else 0 end) as date_num
                from dim.dim_goods as dg
                left join dim.dim_goods_extend as dge
                on dg.goods_id = dge.goods_id
                and dge.data_date >=  from_timestamp(date_sub(now(),7), "yyyyMMdd")
                and dge.data_date <= from_timestamp(date_sub(now(),1), "yyyyMMdd")
                left join dim.dim_goods_category_group_new as cat
                on dg.cate_level1_name = cat.cate_level1_name
                where cat.category_group = '婴童时尚'
                and dg.supplier_genre <> 11
                and dge.is_jc_on_sale=1
                group by 1
            ) as onsale
            on dg.goods_id = onsale.goods_id
            where cat.category_group = '婴童时尚'
            and dg.supplier_genre <> 11
            and onsale.goods_id is not null
            group by 1,2,3,4,5,6,8,9,10,11,12,13,14,15,16,17
        ) as goods
        left join (
            select
                b.goods_id,
                sum(b.original_goods_number) num,
                sum(b.original_goods_number*b.goods_price) revenue,
                sum(b.original_goods_number*b.in_price_usd) +nvl(sum( case when ((lower(a.country_name)='saudi arabia' and e.is_sa_supplier<>1) 
                    or  (lower(a.country_name)='united arab emirates' and e.supplier_genre<>10)) then b.goods_price*b.original_goods_number*0.05
                    when lower(a.country_name) in ('saudi arabia','united arab emirates')
                    then  (b.goods_price-b.in_price_usd)*b.original_goods_number/1.05*0.05 end),0) cost_with_vat
            from dw.dw_order_goods_fact b 
            inner join  dw.dw_order_fact a on a.order_id=b.order_id
            left join dim.dim_goods c on c.goods_id=b.goods_id
            left join jolly.who_esoloo_supplier  e on  c.provider_code = e.code
            left join dim.dim_goods_category_group_new as cat
            on c.cate_level1_name = cat.cate_level1_name
            where
            from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >= from_timestamp(date_sub(now(),7), "yyyyMMdd")
            and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') <= from_timestamp(date_sub(now(),1), "yyyyMMdd")
            and a.site_id  in(400,600,700,601,900) 
            and a.pay_status in(1,3)
            and cat.category_group = '婴童时尚'
            and c.supplier_genre<>11
            group by 1
        ) as sales
        on goods.goods_id = sales.goods_id
        left join (
            select
                cat.category_group,
                dg.goods_id,
                sum(stock.free_stock_num) as free_stock_num
            from ods.ods_who_wms_goods_stock_total_detail as stock
            left join dim.dim_goods as dg
            on stock.goods_id = dg.goods_id
            and stock.data_date = from_timestamp(date_sub(now(),1), "yyyyMMdd")
            left join dim.dim_goods_category_group_new as cat
            on dg.cate_level1_name = cat.cate_level1_name
            where cat.category_group = '婴童时尚'
            group by 1,2
        ) as stock
        on goods.goods_id = stock.goods_id
        left join(
            select
                rpts.goods_id,
                sum(rpts.`7d_expo_pv`) as 30d_expo_pv,
                sum(rpts.`7d_expo_uv`) as 30d_expo_uv,
                sum(rpts.`7d_goods_click_uv`) as goods_click_uv,
                sum(rpts.`7d_sales_uv`) as 30d_sales_uv
            from rpt.rpt_sum_goods_daily as rpts
            left join dim.dim_goods as dg
            on rpts.goods_id = dg.goods_id
            left join dim.dim_goods_category_group_new as cat
            on dg.cate_level1_name = cat.cate_level1_name
            where cat.category_group = '婴童时尚'
            and rpts.data_date = from_timestamp(date_sub(now(),1), "yyyyMMdd")
            group by 1
        ) as rpts
        on goods.goods_id = rpts.goods_id
        left join (
        select 
        patterns.goods_id,
        patterns.material,
        patterns.decoration,
        patterns.color,
        patterns.patterns,
        patterns.collar_type,
        patterns.applicable_gender,
        patterns.applicable_stage,
        patterns.lengths,
        patterns.sleeve_length,
        patterns.sleeve_type
        from (
            select
                patterns.goods_id,
                group_concat(distinct(case when lower(patterns.pattern_key_name) = 'material' then pattern_value else null end),'; ') as material,
                group_concat(distinct(case when lower(patterns.pattern_key_name) = 'decoration' then pattern_value else null end),'; ') as decoration,
                group_concat(distinct(case when lower(patterns.pattern_key_name) = 'color' then pattern_value else null end),'; ') as color,
                group_concat(distinct(case when lower(patterns.pattern_key_name) = 'pattern' then pattern_value else null end),'; ') as patterns,
                group_concat(distinct(case when lower(patterns.pattern_key_name) = 'collar type' then pattern_value else null end),'; ') as collar_type,
                group_concat(distinct(case when lower(patterns.pattern_key_name) = 'applicable gender' then pattern_value else null end),'; ') as applicable_gender,
                group_concat(distinct(case when lower(patterns.pattern_key_name) = 'applicable stage' then pattern_value else null end),'; ') as applicable_stage,
                group_concat(distinct(case when lower(patterns.pattern_key_name) = 'length' then pattern_value else null end),'; ') as lengths,
                group_concat(distinct(case when lower(patterns.pattern_key_name) = 'sleeve length' then pattern_value else null end),'; ') as sleeve_length,
                group_concat(distinct(case when lower(patterns.pattern_key_name) = 'sleeve type' then pattern_value else null end),'; ') as sleeve_type
            from dim.dim_goods_pattern as patterns
            left join  dim.dim_goods as dg
            on patterns.goods_id = dg.goods_id
            left join dim.dim_goods_category_group_new as cat
            on dg.cate_level1_name = cat.cate_level1_name
            where cat.category_group = '婴童时尚'
            and patterns.pattern_value is not null
            group by 1
        ) as patterns
        where patterns.goods_id is not null
        or patterns.material is not null
        or patterns.decoration is not null
        or patterns.color is not null
        or patterns.patterns is not null
        or patterns.collar_type is not null
        or patterns.applicable_gender is not null
        or patterns.applicable_stage is not null
        or patterns.lengths is not null
        or patterns.sleeve_length is not null
        or patterns.sleeve_type is not null
        ) as patterns
        on goods.goods_id = patterns.goods_id
        """
        return basesqlmsg
    elif period == "month":
        basesqlmsg = """
            select
                goods.category_group,
                goods.cate_level1_name,
                goods.cate_level2_name,
                goods.cate_level3_name,
                goods.cate_level4_name,
                goods.goods_link as `链接`,
                goods.goods_id,
                goods.sku_num as `sku数量`,
            
                rpts.`30d_expo_pv` as `曝光数`,
                rpts.goods_click_uv as `点击数`,
                rpts.goods_click_uv/rpts.`30d_expo_uv` as `点击率`,
                rpts.`30d_sales_uv`/rpts.goods_click_uv as `转化率`,
                sales.num as `销量`,
                sales.revenue as `GMV`,
                sales.num/goods.date_num as `日均销量`,
                case when sales.num/goods.date_num < 1 then '滞销'
                    when sales.num/goods.date_num>=1 and sales.num/goods.date_num<3.4 then '平销'
                    when sales.num/goods.date_num>=3.4 then '畅销'
                    else null end as `畅销类型`,
                goods.in_price as `成本(￥)`,
                goods.in_price_usd as `成本($)`,
                goods.prst_price_1 as `销售价($)`,
            
                stock.free_stock_num as `库存`,
                goods.is_jc_sale as `是否可售`,
                goods.is_jc_on_sale as `是否在架`,
                goods.provider_code as `供应商编码`,
                goods.supp_name as `供应商名称`,
                goods.first_on_sale_time as `上架时间`,
                patterns.material,
                patterns.decoration,
                patterns.color,
                patterns.patterns,
                patterns.collar_type,
                patterns.applicable_gender,
                patterns.applicable_stage,
                patterns.lengths,
                patterns.sleeve_length,
                patterns.sleeve_type
            from (
            select
                cat.category_group,
                dg.cate_level1_name,
                dg.cate_level2_name,
                dg.cate_level3_name,
                dg.cate_level4_name,
                dg.goods_id,
                count(sku.rec_id) as sku_num,
                dgi.main_img_url as goods_link,
                dge.is_jc_sale,
                dge.is_jc_on_sale,
                dg.provider_code,
                supp.supp_name,
                dge.in_price,
                dge.in_price_usd,
                dge.prst_price_1,
                dg.first_on_sale_time,
                onsale.date_num
            from dim.dim_goods as dg
            left join dim.dim_goods_extend as dge
            on dg.goods_id = dge.goods_id
            and dge.data_date = from_timestamp(date_sub(now(),1), "yyyyMMdd")
            left join dim.dim_goods_category_group_new as cat
            on dg.cate_level1_name = cat.cate_level1_name
            left join zybiro.bi_elaine_supp_name as supp
            on dg.provider_code = supp.code
            left join jolly.who_sku_relation as sku
            on dg.goods_id = sku.goods_id
            left join dim.dim_goods_image as dgi
            on cast(dg.goods_id as int) = cast(dgi.goods_id as int)
            left join (
                select
                    dg.goods_id,
                    sum(case when dge.is_jc_on_sale=1 then 1 else 0 end) as date_num
                from dim.dim_goods as dg
                left join dim.dim_goods_extend as dge
                on dg.goods_id = dge.goods_id
                and dge.data_date >= from_timestamp(trunc(date_sub(now(),1), "MM"), "yyyyMMdd")
                and dge.data_date <= from_timestamp(date_sub(now(),1), "yyyyMMdd")
                left join dim.dim_goods_category_group_new as cat
                on dg.cate_level1_name = cat.cate_level1_name
                where cat.category_group = '婴童时尚'
                and dg.supplier_genre <> 11
                and dge.is_jc_on_sale=1
                group by 1
            ) as onsale
            on dg.goods_id = onsale.goods_id
            where cat.category_group = '婴童时尚'
            and dg.supplier_genre <> 11
            and onsale.goods_id is not null
            group by 1,2,3,4,5,6,8,9,10,11,12,13,14,15,16,17
            ) as goods
            left join (
            select
                b.goods_id,
                sum(b.original_goods_number) num,
                sum(b.original_goods_number*b.goods_price) revenue,
                sum(b.original_goods_number*b.in_price_usd) +nvl(sum( case when ((lower(a.country_name)='saudi arabia' and e.is_sa_supplier<>1) 
                    or  (lower(a.country_name)='united arab emirates' and e.supplier_genre<>10)) then b.goods_price*b.original_goods_number*0.05
                    when lower(a.country_name) in ('saudi arabia','united arab emirates')
                    then  (b.goods_price-b.in_price_usd)*b.original_goods_number/1.05*0.05 end),0) cost_with_vat
            from dw.dw_order_goods_fact b 
            inner join  dw.dw_order_fact a on a.order_id=b.order_id
            left join dim.dim_goods c on c.goods_id=b.goods_id
            left join jolly.who_esoloo_supplier  e on  c.provider_code = e.code
            left join dim.dim_goods_category_group_new as cat
            on c.cate_level1_name = cat.cate_level1_name
            where
            from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >= from_timestamp(trunc(date_sub(now(),1), "MM"), "yyyyMMdd")
            and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') <= from_timestamp(date_sub(now(),1), "yyyyMMdd")
            and a.site_id  in(400,600,700,601,900) 
            and a.pay_status in(1,3)
            and cat.category_group = '婴童时尚'
            and c.supplier_genre<>11
            group by 1
            ) as sales
            on goods.goods_id = sales.goods_id
            left join (
            select
                cat.category_group,
                dg.goods_id,
                sum(stock.free_stock_num) as free_stock_num
            from ods.ods_who_wms_goods_stock_total_detail as stock
            left join dim.dim_goods as dg
            on stock.goods_id = dg.goods_id
            and stock.data_date = from_timestamp(date_sub(now(),1), "yyyyMMdd")
            left join dim.dim_goods_category_group_new as cat
            on dg.cate_level1_name = cat.cate_level1_name
            where cat.category_group = '婴童时尚'
            group by 1,2
            ) as stock
            on goods.goods_id = stock.goods_id
            left join(
            select
                rpts.goods_id,
                sum(rpts.`30d_expo_pv`) as 30d_expo_pv,
                sum(rpts.`30d_expo_uv`) as 30d_expo_uv,
                sum(rpts.`30d_goods_click_uv`) as goods_click_uv,
                sum(rpts.`30d_sales_uv`) as 30d_sales_uv
            from rpt.rpt_sum_goods_daily as rpts
            left join dim.dim_goods as dg
            on rpts.goods_id = dg.goods_id
            left join dim.dim_goods_category_group_new as cat
            on dg.cate_level1_name = cat.cate_level1_name
            where cat.category_group = '婴童时尚'
            and rpts.data_date = from_timestamp(date_sub(now(),1), "yyyyMMdd")
            group by 1
            ) as rpts
            on goods.goods_id = rpts.goods_id
            left join (
            select 
            patterns.goods_id,
            patterns.material,
            patterns.decoration,
            patterns.color,
            patterns.patterns,
            patterns.collar_type,
            patterns.applicable_gender,
            patterns.applicable_stage,
            patterns.lengths,
            patterns.sleeve_length,
            patterns.sleeve_type
            from (
            select
                patterns.goods_id,
                group_concat(distinct(case when lower(patterns.pattern_key_name) = 'material' then pattern_value else null end),'; ') as material,
                group_concat(distinct(case when lower(patterns.pattern_key_name) = 'decoration' then pattern_value else null end),'; ') as decoration,
                group_concat(distinct(case when lower(patterns.pattern_key_name) = 'color' then pattern_value else null end),'; ') as color,
                group_concat(distinct(case when lower(patterns.pattern_key_name) = 'pattern' then pattern_value else null end),'; ') as patterns,
                group_concat(distinct(case when lower(patterns.pattern_key_name) = 'collar type' then pattern_value else null end),'; ') as collar_type,
                group_concat(distinct(case when lower(patterns.pattern_key_name) = 'applicable gender' then pattern_value else null end),'; ') as applicable_gender,
                group_concat(distinct(case when lower(patterns.pattern_key_name) = 'applicable stage' then pattern_value else null end),'; ') as applicable_stage,
                group_concat(distinct(case when lower(patterns.pattern_key_name) = 'length' then pattern_value else null end),'; ') as lengths,
                group_concat(distinct(case when lower(patterns.pattern_key_name) = 'sleeve length' then pattern_value else null end),'; ') as sleeve_length,
                group_concat(distinct(case when lower(patterns.pattern_key_name) = 'sleeve type' then pattern_value else null end),'; ') as sleeve_type
            from dim.dim_goods_pattern as patterns
            left join  dim.dim_goods as dg
            on patterns.goods_id = dg.goods_id
            left join dim.dim_goods_category_group_new as cat
            on dg.cate_level1_name = cat.cate_level1_name
            where cat.category_group = '婴童时尚'
            and patterns.pattern_value is not null
            group by 1
            ) as patterns
            where patterns.goods_id is not null
            or patterns.material is not null
            or patterns.decoration is not null
            or patterns.color is not null
            or patterns.patterns is not null
            or patterns.collar_type is not null
            or patterns.applicable_gender is not null
            or patterns.applicable_stage is not null
            or patterns.lengths is not null
            or patterns.sleeve_length is not null
            or patterns.sleeve_type is not null
            ) as patterns
            on goods.goods_id = patterns.goods_id
        """
        return basesqlmsg


def kids_week_month_cate_sql(period:str):
    if period == "week":
        basesqlmsg = """
        with goods as (
            select
                a0.category_group,
                a0.cate_level1_name,
                a0.cate_level2_name,
                a0.cate_level3_name,
                a0.cate_level4_name,
                count(distinct case when a0.is_new_goods != 'this_year_new' then a0.goods_id else null end) as old_goods_num,
                count(distinct case when a0.is_new_goods = 'this_year_new' then a0.goods_id else null end) as new_goods_num,
                count(case when a0.is_new_goods != 'this_year_new' then a0.rec_id else null end) as old_sku_num,
                count(case when a0.is_new_goods = 'this_year_new' then a0.rec_id else null end) as new_sku_num
            from(
                select
                    cat.category_group,
                    dg.cate_level1_name,
                    dg.cate_level2_name,
                    dg.cate_level3_name,
                    dg.cate_level4_name,
                    dg.goods_id,
                    case when dg.first_on_sale_time >= trunc(date_sub(now(),1),'MM')
                            and dg.first_on_sale_time < from_timestamp(now(),"yyyy-MM-dd") then 'this_year_new'
                        when dg.first_on_sale_time >= trunc(years_sub(date_sub(now(),1),1),'MM')
                            and dg.first_on_sale_time < from_timestamp(years_sub(now(),1),"yyyy-MM-dd") then 'last_year_new'
                        else "old_goods" end as is_new_goods,
                    sku.rec_id
                from dim.dim_goods as dg
                left join dim.dim_goods_extend as dge
                on dg.goods_id = dge.goods_id
                and dge.data_date = from_timestamp(date_sub(now(),1),"yyyyMMdd")
                left join dim.dim_goods_category_group_new as cat
                on dg.cate_level1_name = cat.cate_level1_name
                left join jolly.who_sku_relation as sku
                on dg.goods_id = sku.goods_id
                where cat.category_group = "婴童时尚"
                and dg.supplier_genre <> 11
                and dge.is_jc_on_sale=1
            ) as a0
            group by 1,2,3,4,5
        ),
        
        sales0 as (
            select
                a0.category_group,
                a0.cate_level1_name,
                a0.cate_level2_name,
                a0.cate_level3_name,
                a0.cate_level4_name,
                avg(case when is_new_goods = "this_year_new" then a0.goods_price else 0 end) as new_goods_avg_price,
                avg(case when is_new_goods != "this_year_new" then a0.goods_price else 0 end) as old_goods_avg_price,
                count(case when a0.period="this_period" and is_new_goods = "this_year_new" then a0.goods_id else null end) as this_period_new_goods_num,
                count(case when a0.period="this_period" and is_new_goods != "this_year_new" then a0.goods_id else null end) as this_period_old_goods_num,
                sum(case when a0.period="this_period" and is_new_goods = "this_year_new" then a0.revenue else 0 end) as this_period_new_goods_gmv,
                sum(case when a0.period="this_period" and is_new_goods != "this_year_new" then a0.revenue else 0 end) as this_period_old_goods_gmv,
                sum(case when a0.period="last_period" and is_new_goods = "last_year_new" then a0.revenue else 0 end) as last_period_new_goods_gmv,
                sum(case when a0.period="last_period" and is_new_goods != "last_year_new" then a0.revenue else 0 end) as last_period_old_goods_gmv,
                
                sum(case when a0.period="this_period" and is_new_goods = "this_year_new" then a0.num else 0 end) as this_period_new_nums,
                sum(case when a0.period="this_period" and is_new_goods != "this_year_new" then a0.num else 0 end) as this_period_old_nums,
                sum(case when a0.period="last_period" and is_new_goods = "last_year_new" then a0.num else 0 end) as last_period_new_nums,
                sum(case when a0.period="last_period" and is_new_goods != "last_year_new" then a0.num else 0 end) as last_period_old_nums,
                count(distinct case when a0.period="this_period" and a0.is_new_goods = "this_year_new" and a0.unit_day_num>=3.4 then a0.goods_id else null end) as this_new_good_sale_num,
                count(distinct case when a0.period="this_period" and a0.is_new_goods = "this_year_new" and a0.unit_day_num>=1 and a0.unit_day_num<3.4 then a0.goods_id else null end) as this_new_unit_sale_num,
                count(distinct case when a0.period="this_period" and a0.is_new_goods = "this_year_new" and a0.unit_day_num<1 then a0.goods_id else null end) as this_new_bad_sale_num,
                
                count(distinct case when a0.period="this_period" and a0.is_new_goods != "this_year_new" and a0.unit_day_num>=3.4 then a0.goods_id else null end) as this_old_good_sale_num,
                count(distinct case when a0.period="this_period" and a0.is_new_goods != "this_year_new" and a0.unit_day_num>=1 and a0.unit_day_num<3.4 then a0.goods_id else null end) as this_old_unit_sale_num,
                count(distinct case when a0.period="this_period" and a0.is_new_goods != "this_year_new" and a0.unit_day_num<1 then a0.goods_id else null end) as this_old_bad_sale_num,
                
                sum(case when a0.period="last_year_period" and is_new_goods = "last_year_new" then a0.revenue else 0 end) as last_year_period_new_goods_gmv,
                sum(case when a0.period="last_year_period" and is_new_goods != "last_year_new" then a0.revenue else 0 end) as last_year_period_old_goods_gmv,
                
                1-sum(case when a0.period="this_period" and is_new_goods = "this_year_new" then a0.cost_with_vat else 0 end)/
                    sum(case when a0.period="this_period" and is_new_goods = "this_year_new" then a0.revenue else 0 end) as this_new_goods_margin,
                1-sum(case when a0.period="this_period" and is_new_goods != "this_year_new" then a0.cost_with_vat else 0 end)/
                    sum(case when a0.period="this_period" and is_new_goods != "this_year_new" then a0.revenue else 0 end) as this_old_goods_margin
            from(
                select
                    a0.period,
                    a0.category_group,
                    a0.cate_level1_name,
                    a0.cate_level2_name,
                    a0.cate_level3_name,
                    a0.cate_level4_name,
                    a0.goods_id,
                    a0.is_new_goods,
                    a0.goods_price,
                    a1.on_sale_days,
                    sum(a0.num) as num,
                    sum(a0.revenue) as revenue,
                    sum(a0.cost_with_vat) as cost_with_vat,
                    sum(a0.num)/a1.on_sale_days as unit_day_num
                from(
                    select
                        from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') data_date,
                        case when from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd')
                                            >= from_timestamp(date_sub(trunc(now(),'d'),7), "yyyyMMdd")
                                and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd')
                                            < from_timestamp(trunc(now(),'d'), "yyyyMMdd") then 'last_period'
                            when from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd')
                                            >= from_timestamp(trunc(now(),'d'), "yyyyMMdd")
                                and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd')
                                            <= from_timestamp(date_sub(now(),1),'yyyyMMdd') then 'this_period'
                            when from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd')
                                            >= from_timestamp(years_sub(trunc(now(),'d'),1),'yyyyMMdd')
                                and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd')
                                            <= from_timestamp(years_sub(date_sub(now(),1),1),"yyyyMMdd") then 'last_year_period'
                            else null end as period,
                        cat.category_group,
                        case when c.cate_level1_name='Beauty' then c.cate_level2_name else c.cate_level1_name end as cate_level1_name,
                        case when c.cate_level2_name='Beauty' then c.cate_level3_name else c.cate_level2_name end as cate_level2_name,
                        case when c.cate_level3_name='Beauty' then c.cate_level4_name else c.cate_level3_name end as cate_level3_name,
                        case when c.cate_level4_name='Beauty' then c.cate_level5_name else c.cate_level4_name end as cate_level4_name,
                        b.goods_id,
                        case when c.first_on_sale_time >= trunc(date_sub(now(),1),'MM')
                                and c.first_on_sale_time < from_timestamp(now(),"yyyy-MM-dd") then 'this_year_new'
                            when c.first_on_sale_time >= trunc(years_sub(date_sub(now(),1),1),'MM')
                                and c.first_on_sale_time < from_timestamp(years_sub(now(),1),"yyyy-MM-dd") then 'last_year_new'
                            else "old_goods" end as is_new_goods,
                        avg(b.goods_price) as goods_price,
                        sum(b.original_goods_number) num,
                        sum(b.original_goods_number*b.goods_price) revenue,
                        sum(b.original_goods_number*b.in_price_usd) +nvl(sum( case when ((lower(a.country_name)='saudi arabia' and e.is_sa_supplier<>1) 
                            or  (lower(a.country_name)='united arab emirates' and e.supplier_genre<>10)) then b.goods_price*b.original_goods_number*0.05
                            when lower(a.country_name) in ('saudi arabia','united arab emirates')
                            then  (b.goods_price-b.in_price_usd)*b.original_goods_number/1.05*0.05 end),0) cost_with_vat
                    from dw.dw_order_goods_fact b 
                    inner join  dw.dw_order_fact a on a.order_id=b.order_id
                    left join dim.dim_goods c on c.goods_id=b.goods_id
                    left join jolly.who_esoloo_supplier  e on  c.provider_code = e.code
                    left join dim.dim_goods_category_group_new as cat
                    on c.cate_level1_name = cat.cate_level1_name
                    where
                    ((from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >= from_timestamp(date_sub(now(),7),'yyyyMMdd')
                        and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') <= from_timestamp(date_sub(now(),1),'yyyyMMdd') )
                    or (from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >= from_timestamp(years_sub(date_sub(now(),7),1),"yyyyMMdd")
                    and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') <= from_timestamp(years_sub(date_sub(now(),1),1),"yyyyMMdd") ))
                    and a.site_id  in(400,600,700,601,900) 
                    and a.pay_status in(1,3)
                    and cat.category_group = '婴童时尚'
                    and c.supplier_genre<>11
                    group by 1,2,3,4,5,6,7,8,9
                ) as a0
                left join (
                    select
                        dg.goods_id,
                        sum(dge.is_jc_on_sale) as on_sale_days
                    from dim.dim_goods as dg
                    left join dim.dim_goods_extend as dge
                    on dg.goods_id = dge.goods_id
                    left join dim.dim_goods_category_group_new as cat
                    on dg.cate_level1_name = cat.cate_level1_name
                    where cat.category_group = "婴童时尚"
                    and dge.data_date >= from_timestamp(date_sub(now(),7),'yyyyMMdd')
                    and dge.data_date <= from_timestamp(date_sub(now(),1),'yyyyMMdd')
                    and dge.is_jc_on_sale = 1
                    group by 1
                ) as a1
                on a0.goods_id = a1.goods_id
                group by 1,2,3,4,5,6,7,8,9,10
            ) as a0
            group by 1,2,3,4,5
        ),
        
        dau as (
            select
                a0.category_group,
                a0.cate_level1_name,
                a0.cate_level2_name,
                a0.cate_level3_name,
                a0.cate_level4_name,
                sum(case when a0.is_new_goods = "this_year_new" then 7d_expo_pv else 0 end) as expo_pv_new,
                sum(case when a0.is_new_goods = "this_year_new" then 7d_expo_uv else 0 end) as expo_uv_new,
                sum(case when a0.is_new_goods = "this_year_new" then 7d_goods_click_uv else 0 end) as goods_click_uv_new,
                sum(case when a0.is_new_goods = "this_year_new" then 7d_goods_click else 0 end) as goods_click_new,
                sum(case when a0.is_new_goods = "this_year_new" then 7d_sales_uv else 0 end) as sales_uv_new,
                sum(case when a0.is_new_goods != "this_year_new" then 7d_expo_pv else 0 end) as expo_pv_old,
                sum(case when a0.is_new_goods != "this_year_new" then 7d_expo_uv else 0 end) as expo_uv_old,
                sum(case when a0.is_new_goods != "this_year_new" then 7d_goods_click_uv else 0 end) as goods_click_uv_old,
                sum(case when a0.is_new_goods != "this_year_new" then 7d_goods_click else 0 end) as goods_click_old,
                sum(case when a0.is_new_goods != "this_year_new" then 7d_sales_uv else 0 end) as sales_uv_old
            from(
                select
                    cat.category_group,
                    dg.cate_level1_name,
                    dg.cate_level2_name,
                    dg.cate_level3_name,
                    dg.cate_level4_name,
                    dg.goods_id,
                    case when dg.first_on_sale_time >= trunc(date_sub(now(),1),'MM')
                            and dg.first_on_sale_time < from_timestamp(now(),"yyyy-MM-dd") then 'this_year_new'
                        when dg.first_on_sale_time >= trunc(years_sub(date_sub(now(),1),1),'MM')
                            and dg.first_on_sale_time < from_timestamp(years_sub(now(),1),"yyyy-MM-dd") then 'last_year_new'
                        else "old_goods" end as is_new_goods,
                    rpts.data_date,
                    nvl(rpts.`7d_expo_pv`,0) as `7d_expo_pv`,
                    nvl(rpts.`7d_expo_uv`,0) as `7d_expo_uv`,
                    nvl(rpts.`7d_goods_click_uv`,0) as `7d_goods_click_uv`,
                    nvl(rpts.`7d_goods_click`,0) as `7d_goods_click`,
                    nvl(rpts.`7d_sales_uv`,0) as `7d_sales_uv`
                from rpt.rpt_sum_goods_daily as rpts
                left join dim.dim_goods as dg
                on rpts.goods_id = dg.goods_id
                left join dim.dim_goods_category_group_new as cat
                on dg.cate_level1_name = cat.cate_level1_name
                where cat.category_group = "婴童时尚"
                and rpts.data_date = from_timestamp(date_sub(now(),1), "yyyyMMdd")
                and dg.supplier_genre <> 11
                and rpts.site_id in (400,600,700,601,900)
            ) as a0
            group by 1,2,3,4,5
        )
        
        
        select
            goods.category_group,
            goods.cate_level1_name,
            goods.cate_level2_name,
            goods.cate_level3_name,
            goods.cate_level4_name,
            goods.new_goods_num as `新品在架商品数`,
            goods.new_sku_num as `新品在架sku数`,
            sales0.this_period_new_goods_num as `新品销售商品数`,
            sales0.this_period_new_goods_num/goods.new_goods_num as `新品动销率`,
            sales0.this_period_new_goods_gmv as `新品销售额`,
            sales0.last_period_new_goods_gmv as `上期新品销售额`,
            sales0.this_period_new_goods_gmv/sales0.last_period_new_goods_gmv-1 as `销售环比`,
            sales0.this_period_new_nums as `销量`,
            sales0.last_period_new_nums as `上期新品销量`,
            sales0.this_period_new_nums/sales0.last_period_new_nums-1 as `销量环比`,
            sales0.last_year_period_new_goods_gmv as `同期新品销售额`,
            sales0.new_goods_avg_price as `新品销售均价`,
            dau.expo_pv_new as `新品曝光`,
            dau.goods_click_uv_new as `新品uv`,
            sales0.this_period_new_goods_gmv/dau.goods_click_uv_new as `新品uv产出`,
            dau.goods_click_uv_new/dau.expo_uv_new as `新品点击率`,
            dau.sales_uv_new/dau.goods_click_uv_new as `新品转化率`,
            sales0.this_new_goods_margin as `新品毛利率`,
            sales0.this_new_good_sale_num as `新品畅销款数量`,
            sales0.this_new_good_sale_num/goods.new_goods_num as `新品畅销款占比`,
            sales0.this_new_unit_sale_num as `新品平销款数量`,
            sales0.this_new_unit_sale_num/goods.new_goods_num as `新品平销款占比`,
            sales0.this_new_bad_sale_num as `新品滞销款数量`,
            sales0.this_new_bad_sale_num/goods.new_goods_num as `新品滞销款占比`,
        
        
            goods.old_goods_num as `老品在架商品数`,
            goods.old_sku_num as `老品在架sku数`,
            sales0.this_period_old_goods_num as `老品销售商品数`,
            sales0.this_period_old_goods_num/goods.old_goods_num as `老品动销率`,
            sales0.this_period_old_goods_gmv as `老品销售额`,
            sales0.last_period_old_goods_gmv as `上期老品销售额`,
            sales0.this_period_old_goods_gmv/sales0.last_period_old_goods_gmv-1 as `销售环比`,
            sales0.this_period_old_nums as `销量`,
            sales0.last_period_old_nums as `上期老品销量`,
            sales0.this_period_old_nums/sales0.last_period_old_nums-1 as `销量环比`,
            sales0.last_year_period_old_goods_gmv as `同期老品销售额`,
            sales0.old_goods_avg_price as `老品销售均价`,
            dau.expo_pv_old as `老品曝光`,
            dau.goods_click_uv_old as `老品uv`,
            sales0.this_period_old_goods_gmv/dau.goods_click_uv_old as `老品uv产出`,
            dau.goods_click_uv_old/dau.expo_uv_old as `老品点击率`,
            dau.sales_uv_old/dau.goods_click_uv_old as `老品转化率`,
            sales0.this_old_goods_margin as `老品毛利率`,
            sales0.this_old_good_sale_num as `老品畅销款数量`,
            sales0.this_old_good_sale_num/goods.old_goods_num as `老品畅销款占比`,
            sales0.this_old_unit_sale_num as `老品平销款数量`,
            sales0.this_old_unit_sale_num/goods.old_goods_num as `老品平销款占比`,
            sales0.this_old_bad_sale_num as `老品滞销款数量`,
            sales0.this_old_bad_sale_num/goods.old_goods_num as `老品滞销款占比`
        from goods left join sales0
        on goods.category_group = sales0.category_group
        and goods.cate_level1_name = sales0.cate_level1_name
        and goods.cate_level2_name = sales0.cate_level2_name
        and goods.cate_level3_name = sales0.cate_level3_name
        and goods.cate_level4_name = sales0.cate_level4_name
        left join dau
        on goods.category_group = dau.category_group
        and goods.cate_level1_name = dau.cate_level1_name
        and goods.cate_level2_name = dau.cate_level2_name
        and goods.cate_level3_name = dau.cate_level3_name
        and goods.cate_level4_name = dau.cate_level4_name
        """
        return basesqlmsg
    elif period == "month":
        basesqlmsg = """
        with goods as (
            select
                a0.category_group,
                a0.cate_level1_name,
                a0.cate_level2_name,
                a0.cate_level3_name,
                a0.cate_level4_name,
                count(distinct case when a0.is_new_goods != 'this_year_new' then a0.goods_id else null end) as old_goods_num,
                count(distinct case when a0.is_new_goods = 'this_year_new' then a0.goods_id else null end) as new_goods_num,
                count(case when a0.is_new_goods != 'this_year_new' then a0.rec_id else null end) as old_sku_num,
                count(case when a0.is_new_goods = 'this_year_new' then a0.rec_id else null end) as new_sku_num
            from(
                select
                    cat.category_group,
                    dg.cate_level1_name,
                    dg.cate_level2_name,
                    dg.cate_level3_name,
                    dg.cate_level4_name,
                    dg.goods_id,
                    case when dg.first_on_sale_time >= trunc(date_sub(now(),1),'MM')
                            and dg.first_on_sale_time < from_timestamp(now(),"yyyy-MM-dd") then 'this_year_new'
                        when dg.first_on_sale_time >= trunc(years_sub(date_sub(now(),1),1),'MM')
                            and dg.first_on_sale_time < from_timestamp(years_sub(now(),1),"yyyy-MM-dd") then 'last_year_new'
                        else "old_goods" end as is_new_goods,
                    sku.rec_id
                from dim.dim_goods as dg
                left join dim.dim_goods_extend as dge
                on dg.goods_id = dge.goods_id
                and dge.data_date = from_timestamp(date_sub(now(),1),"yyyyMMdd")
                left join dim.dim_goods_category_group_new as cat
                on dg.cate_level1_name = cat.cate_level1_name
                left join jolly.who_sku_relation as sku
                on dg.goods_id = sku.goods_id
                where cat.category_group = "婴童时尚"
                and dg.supplier_genre <> 11
                and dge.is_jc_on_sale=1
            ) as a0
            group by 1,2,3,4,5
        ),
        
        sales0 as (
            select
                a0.category_group,
                a0.cate_level1_name,
                a0.cate_level2_name,
                a0.cate_level3_name,
                a0.cate_level4_name,
                avg(case when is_new_goods = "this_year_new" then a0.goods_price else 0 end) as new_goods_avg_price,
                avg(case when is_new_goods != "this_year_new" then a0.goods_price else 0 end) as old_goods_avg_price,
                count(case when a0.period="this_period" and is_new_goods = "this_year_new" then a0.goods_id else null end) as this_period_new_goods_num,
                count(case when a0.period="this_period" and is_new_goods != "this_year_new" then a0.goods_id else null end) as this_period_old_goods_num,
                sum(case when a0.period="this_period" and is_new_goods = "this_year_new" then a0.revenue else 0 end) as this_period_new_goods_gmv,
                sum(case when a0.period="this_period" and is_new_goods != "this_year_new" then a0.revenue else 0 end) as this_period_old_goods_gmv,
                sum(case when a0.period="last_period" and is_new_goods = "last_year_new" then a0.revenue else 0 end) as last_period_new_goods_gmv,
                sum(case when a0.period="last_period" and is_new_goods != "last_year_new" then a0.revenue else 0 end) as last_period_old_goods_gmv,
                
                sum(case when a0.period="this_period" and is_new_goods = "this_year_new" then a0.num else 0 end) as this_period_new_nums,
                sum(case when a0.period="this_period" and is_new_goods != "this_year_new" then a0.num else 0 end) as this_period_old_nums,
                sum(case when a0.period="last_period" and is_new_goods = "last_year_new" then a0.num else 0 end) as last_period_new_nums,
                sum(case when a0.period="last_period" and is_new_goods != "last_year_new" then a0.num else 0 end) as last_period_old_nums,
                count(distinct case when a0.period="this_period" and a0.is_new_goods = "this_year_new" and a0.unit_day_num>=3.4 then a0.goods_id else null end) as this_new_good_sale_num,
                count(distinct case when a0.period="this_period" and a0.is_new_goods = "this_year_new" and a0.unit_day_num>=1 and a0.unit_day_num<3.4 then a0.goods_id else null end) as this_new_unit_sale_num,
                count(distinct case when a0.period="this_period" and a0.is_new_goods = "this_year_new" and a0.unit_day_num<1 then a0.goods_id else null end) as this_new_bad_sale_num,
                
                count(distinct case when a0.period="this_period" and a0.is_new_goods != "this_year_new" and a0.unit_day_num>=3.4 then a0.goods_id else null end) as this_old_good_sale_num,
                count(distinct case when a0.period="this_period" and a0.is_new_goods != "this_year_new" and a0.unit_day_num>=1 and a0.unit_day_num<3.4 then a0.goods_id else null end) as this_old_unit_sale_num,
                count(distinct case when a0.period="this_period" and a0.is_new_goods != "this_year_new" and a0.unit_day_num<1 then a0.goods_id else null end) as this_old_bad_sale_num,
                
                sum(case when a0.period="last_year_period" and is_new_goods = "last_year_new" then a0.revenue else 0 end) as last_year_period_new_goods_gmv,
                sum(case when a0.period="last_year_period" and is_new_goods != "last_year_new" then a0.revenue else 0 end) as last_year_period_old_goods_gmv,
                
                1-sum(case when a0.period="this_period" and is_new_goods = "this_year_new" then a0.cost_with_vat else 0 end)/
                    sum(case when a0.period="this_period" and is_new_goods = "this_year_new" then a0.revenue else 0 end) as this_new_goods_margin,
                1-sum(case when a0.period="this_period" and is_new_goods != "this_year_new" then a0.cost_with_vat else 0 end)/
                    sum(case when a0.period="this_period" and is_new_goods != "this_year_new" then a0.revenue else 0 end) as this_old_goods_margin
            from(
                select
                    a0.period,
                    a0.category_group,
                    a0.cate_level1_name,
                    a0.cate_level2_name,
                    a0.cate_level3_name,
                    a0.cate_level4_name,
                    a0.goods_id,
                    a0.is_new_goods,
                    a0.goods_price,
                    a1.on_sale_days,
                    sum(a0.num) as num,
                    sum(a0.revenue) as revenue,
                    sum(a0.cost_with_vat) as cost_with_vat,
                    sum(a0.num)/a1.on_sale_days as unit_day_num
                from(
                    select
                        from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') data_date,
                        case when from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd')
                                            >= from_timestamp(date_sub(trunc(now(),'d'),7), "yyyyMMdd")
                                and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd')
                                            < from_timestamp(trunc(now(),'d'), "yyyyMMdd") then 'last_period'
                            when from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd')
                                            >= from_timestamp(trunc(now(),'d'), "yyyyMMdd")
                                and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd')
                                            <= from_timestamp(date_sub(now(),1),'yyyyMMdd') then 'this_period'
                            when from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd')
                                            >= from_timestamp(years_sub(trunc(now(),'d'),1),'yyyyMMdd')
                                and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd')
                                            <= from_timestamp(years_sub(date_sub(now(),1),1),"yyyyMMdd") then 'last_year_period'
                            else null end as period,
                        cat.category_group,
                        case when c.cate_level1_name='Beauty' then c.cate_level2_name else c.cate_level1_name end as cate_level1_name,
                        case when c.cate_level2_name='Beauty' then c.cate_level3_name else c.cate_level2_name end as cate_level2_name,
                        case when c.cate_level3_name='Beauty' then c.cate_level4_name else c.cate_level3_name end as cate_level3_name,
                        case when c.cate_level4_name='Beauty' then c.cate_level5_name else c.cate_level4_name end as cate_level4_name,
                        b.goods_id,
                        case when c.first_on_sale_time >= trunc(date_sub(now(),1),'MM')
                                and c.first_on_sale_time < from_timestamp(now(),"yyyy-MM-dd") then 'this_year_new'
                            when c.first_on_sale_time >= trunc(years_sub(date_sub(now(),1),1),'MM')
                                and c.first_on_sale_time < from_timestamp(years_sub(now(),1),"yyyy-MM-dd") then 'last_year_new'
                            else "old_goods" end as is_new_goods,
                        avg(b.goods_price) as goods_price,
                        sum(b.original_goods_number) num,
                        sum(b.original_goods_number*b.goods_price) revenue,
                        sum(b.original_goods_number*b.in_price_usd) +nvl(sum( case when ((lower(a.country_name)='saudi arabia' and e.is_sa_supplier<>1) 
                            or  (lower(a.country_name)='united arab emirates' and e.supplier_genre<>10)) then b.goods_price*b.original_goods_number*0.05
                            when lower(a.country_name) in ('saudi arabia','united arab emirates')
                            then  (b.goods_price-b.in_price_usd)*b.original_goods_number/1.05*0.05 end),0) cost_with_vat
                    from dw.dw_order_goods_fact b 
                    inner join  dw.dw_order_fact a on a.order_id=b.order_id
                    left join dim.dim_goods c on c.goods_id=b.goods_id
                    left join jolly.who_esoloo_supplier  e on  c.provider_code = e.code
                    left join dim.dim_goods_category_group_new as cat
                    on c.cate_level1_name = cat.cate_level1_name
                    where
                    ((from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >= from_timestamp(trunc(date_sub(now(),32),'MM'), "yyyyMMdd")
                        and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') <= from_timestamp(date_sub(now(),1),'yyyyMMdd') )
                    or (from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >=from_timestamp(years_sub(trunc(date_sub(now(),1),"MM"),1),'yyyyMMdd')
                    and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') <=from_timestamp(years_sub(date_sub(now(),1),1),"yyyyMMdd") ))
                    and a.site_id  in(400,600,700,601,900) 
                    and a.pay_status in(1,3)
                    and cat.category_group = '婴童时尚'
                    and c.supplier_genre<>11
                    group by 1,2,3,4,5,6,7,8,9
                ) as a0
                left join (
                    select
                        dg.goods_id,
                        sum(dge.is_jc_on_sale) as on_sale_days
                    from dim.dim_goods as dg
                    left join dim.dim_goods_extend as dge
                    on dg.goods_id = dge.goods_id
                    left join dim.dim_goods_category_group_new as cat
                    on dg.cate_level1_name = cat.cate_level1_name
                    where cat.category_group = "婴童时尚"
                    and dge.data_date >= from_timestamp(trunc(date_sub(now(),1),"MM"), "yyyyMMdd")
                    and dge.data_date <= from_timestamp(date_sub(now(),1),'yyyyMMdd')
                    and dge.is_jc_on_sale = 1
                    group by 1
                ) as a1
                on a0.goods_id = a1.goods_id
                group by 1,2,3,4,5,6,7,8,9,10
            ) as a0
            group by 1,2,3,4,5
        ),
        
        dau as (
            select
                a0.category_group,
                a0.cate_level1_name,
                a0.cate_level2_name,
                a0.cate_level3_name,
                a0.cate_level4_name,
                sum(case when a0.is_new_goods = "this_year_new" then 7d_expo_pv else 0 end) as expo_pv_new,
                sum(case when a0.is_new_goods = "this_year_new" then 7d_expo_uv else 0 end) as expo_uv_new,
                sum(case when a0.is_new_goods = "this_year_new" then 7d_goods_click_uv else 0 end) as goods_click_uv_new,
                sum(case when a0.is_new_goods = "this_year_new" then 7d_goods_click else 0 end) as goods_click_new,
                sum(case when a0.is_new_goods = "this_year_new" then 7d_sales_uv else 0 end) as sales_uv_new,
                sum(case when a0.is_new_goods != "this_year_new" then 7d_expo_pv else 0 end) as expo_pv_old,
                sum(case when a0.is_new_goods != "this_year_new" then 7d_expo_uv else 0 end) as expo_uv_old,
                sum(case when a0.is_new_goods != "this_year_new" then 7d_goods_click_uv else 0 end) as goods_click_uv_old,
                sum(case when a0.is_new_goods != "this_year_new" then 7d_goods_click else 0 end) as goods_click_old,
                sum(case when a0.is_new_goods != "this_year_new" then 7d_sales_uv else 0 end) as sales_uv_old
            from(
                select
                    cat.category_group,
                    dg.cate_level1_name,
                    dg.cate_level2_name,
                    dg.cate_level3_name,
                    dg.cate_level4_name,
                    dg.goods_id,
                    case when dg.first_on_sale_time >= trunc(date_sub(now(),1),'MM')
                            and dg.first_on_sale_time < from_timestamp(now(),"yyyy-MM-dd") then 'this_year_new'
                        when dg.first_on_sale_time >= trunc(years_sub(date_sub(now(),1),1),'MM')
                            and dg.first_on_sale_time < from_timestamp(years_sub(now(),1),"yyyy-MM-dd") then 'last_year_new'
                        else "old_goods" end as is_new_goods,
                    rpts.data_date,
                    nvl(rpts.`7d_expo_pv`,0) as `7d_expo_pv`,
                    nvl(rpts.`7d_expo_uv`,0) as `7d_expo_uv`,
                    nvl(rpts.`7d_goods_click_uv`,0) as `7d_goods_click_uv`,
                    nvl(rpts.`7d_goods_click`,0) as `7d_goods_click`,
                    nvl(rpts.`7d_sales_uv`,0) as `7d_sales_uv`
                from rpt.rpt_sum_goods_daily as rpts
                left join dim.dim_goods as dg
                on rpts.goods_id = dg.goods_id
                left join dim.dim_goods_category_group_new as cat
                on dg.cate_level1_name = cat.cate_level1_name
                where cat.category_group = "婴童时尚"
                and rpts.data_date = from_timestamp(date_sub(now(),1), "yyyyMMdd")
                and dg.supplier_genre <> 11
                and rpts.site_id in (400,600,700,601,900)
            ) as a0
            group by 1,2,3,4,5
        )
        
        
        select
            goods.category_group,
            goods.cate_level1_name,
            goods.cate_level2_name,
            goods.cate_level3_name,
            goods.cate_level4_name,
            goods.new_goods_num as `新品在架商品数`,
            goods.new_sku_num as `新品在架sku数`,
            sales0.this_period_new_goods_num as `新品销售商品数`,
            sales0.this_period_new_goods_num/goods.new_goods_num as `新品动销率`,
            sales0.this_period_new_goods_gmv as `新品销售额`,
            sales0.last_period_new_goods_gmv as `上期新品销售额`,
            sales0.this_period_new_goods_gmv/sales0.last_period_new_goods_gmv-1 as `销售环比`,
            sales0.this_period_new_nums as `销量`,
            sales0.last_period_new_nums as `上期新品销量`,
            sales0.this_period_new_nums/sales0.last_period_new_nums-1 as `销量环比`,
            sales0.last_year_period_new_goods_gmv as `同期新品销售额`,
            sales0.new_goods_avg_price as `新品销售均价`,
            dau.expo_pv_new as `新品曝光`,
            dau.goods_click_uv_new as `新品uv`,
            sales0.this_period_new_goods_gmv/dau.goods_click_uv_new as `新品uv产出`,
            dau.goods_click_uv_new/dau.expo_uv_new as `新品点击率`,
            dau.sales_uv_new/dau.goods_click_uv_new as `新品转化率`,
            sales0.this_new_goods_margin as `新品毛利率`,
            sales0.this_new_good_sale_num as `新品畅销款数量`,
            sales0.this_new_good_sale_num/goods.new_goods_num as `新品畅销款占比`,
            sales0.this_new_unit_sale_num as `新品平销款数量`,
            sales0.this_new_unit_sale_num/goods.new_goods_num as `新品平销款占比`,
            sales0.this_new_bad_sale_num as `新品滞销款数量`,
            sales0.this_new_bad_sale_num/goods.new_goods_num as `新品滞销款占比`,
        
        
            goods.old_goods_num as `老品在架商品数`,
            goods.old_sku_num as `老品在架sku数`,
            sales0.this_period_old_goods_num as `老品销售商品数`,
            sales0.this_period_old_goods_num/goods.old_goods_num as `老品动销率`,
            sales0.this_period_old_goods_gmv as `老品销售额`,
            sales0.last_period_old_goods_gmv as `上期老品销售额`,
            sales0.this_period_old_goods_gmv/sales0.last_period_old_goods_gmv-1 as `销售环比`,
            sales0.this_period_old_nums as `销量`,
            sales0.last_period_old_nums as `上期老品销量`,
            sales0.this_period_old_nums/sales0.last_period_old_nums-1 as `销量环比`,
            sales0.last_year_period_old_goods_gmv as `同期老品销售额`,
            sales0.old_goods_avg_price as `老品销售均价`,
            dau.expo_pv_old as `老品曝光`,
            dau.goods_click_uv_old as `老品uv`,
            sales0.this_period_old_goods_gmv/dau.goods_click_uv_old as `老品uv产出`,
            dau.goods_click_uv_old/dau.expo_uv_old as `老品点击率`,
            dau.sales_uv_old/dau.goods_click_uv_old as `老品转化率`,
            sales0.this_old_goods_margin as `老品毛利率`,
            sales0.this_old_good_sale_num as `老品畅销款数量`,
            sales0.this_old_good_sale_num/goods.old_goods_num as `老品畅销款占比`,
            sales0.this_old_unit_sale_num as `老品平销款数量`,
            sales0.this_old_unit_sale_num/goods.old_goods_num as `老品平销款占比`,
            sales0.this_old_bad_sale_num as `老品滞销款数量`,
            sales0.this_old_bad_sale_num/goods.old_goods_num as `老品滞销款占比`
        from goods left join sales0
        on goods.category_group = sales0.category_group
        and goods.cate_level1_name = sales0.cate_level1_name
        and goods.cate_level2_name = sales0.cate_level2_name
        and goods.cate_level3_name = sales0.cate_level3_name
        and goods.cate_level4_name = sales0.cate_level4_name
        left join dau
        on goods.category_group = dau.category_group
        and goods.cate_level1_name = dau.cate_level1_name
        and goods.cate_level2_name = dau.cate_level2_name
        and goods.cate_level3_name = dau.cate_level3_name
        and goods.cate_level4_name = dau.cate_level4_name
        """
        return basesqlmsg


def rpts_daily_2018(begin: str, end: str):
    sql_msg = """
    insert into table zybiro.bi_longjy_sum_goods_daily_2018
    select
        dau.goods_id,
        dau.data_date,
        sum(dau.expo_pv) as expo_pv,
        sum(dau.expo_uv) as expo_uv,
        sum(dau.goods_click) as goods_click,
        sum(dau.goods_click_uv) as goods_click_uv,
        sum(dau.cart_click) as cart_click,
        sum(dau.cart_click_uv) as cart_click_uv,
        sum(dau.favorites_click) as favorites_click,
        sum(dau.favorites_click_uv) as favorites_click_uv,
        sum(dau.bill_uv) as bill_uv,
        sum(dau.sales_uv) as sales_uv,
        sum(dau.`7d_expo_pv`) as `7d_expo_pv`,
        sum(dau.`7d_expo_uv`) as `7d_expo_uv`,
        sum(dau.`7d_goods_click`) as `7d_goods_click`,
        sum(dau.`7d_goods_click_uv`) as `7d_goods_click_uv`,
        sum(dau.`7d_cart_click`) as `7d_cart_click`,
        sum(dau.`7d_cart_click_uv`) as `7d_cart_click_uv`,
        sum(dau.`7d_favorites_click`) as `7d_favorites_click`,
        sum(dau.`7d_favorites_click_uv`) as `7d_favorites_click_uv`,
        sum(dau.`7d_bill`) as `7d_bill`,
        sum(dau.`7d_bill_uv`) as `7d_bill_uv`,
        sum(dau.`7d_sales_uv`) as `7d_sales_uv`,
        sum(dau.`30d_expo_pv`) as `30d_expo_pv`,
        sum(dau.`30d_expo_uv`) as `30d_expo_uv`,
        sum(dau.`30d_goods_click`) as `30d_goods_click`,
        sum(dau.`30d_goods_click_uv`) as `30d_goods_click_uv`,
        sum(dau.`30d_cart_click`) as `30d_cart_click`,
        sum(dau.`30d_cart_click_uv`) as `30d_cart_click_uv`,
        sum(dau.`30d_favorites_click`) as `30d_favorites_click`,
        sum(dau.`30d_favorites_click_uv`) as `30d_favorites_click_uv`,
        sum(dau.`30d_bill`) as `30d_bill`,
        sum(dau.`30d_bill_uv`) as `30d_bill_uv`,
        sum(dau.`30d_sales_uv`) as `30d_sales_uv`
    from gcs.rpt_sum_goods_daily as dau
    where dau.data_date >= '{bg}'
    and dau.data_date < '{ed}'
    group by dau.goods_id,
        dau.data_date
    """.format(bg=begin, ed=end)
    return sql_msg


kids_value = """
with goods_base as (
    select
        base.cate_level1_name,
        base.cate_level2_name,
        base.cate_level3_name,
        base.cate_level4_name,
        base.goods_id,
        base.goods_name,
        val.value,
        val.unit,
        gender.pattern_value
    from(
        select
            dg.cate_level1_name,
            dg.cate_level2_name,
            dg.cate_level3_name,
            dg.cate_level4_name,
            dg.goods_id,
            dg.goods_name
        from dim.dim_goods as dg
        where cate_level1_name = "Kid's Clothing"
    ) as base
    left join (
        select
            values0.goods_id,
            min(value) as value,
            unit
        from(
            select
                wsr.goods_id,
                cast(regexp_extract(wsr.sku_value,"\\d+",0) as int) as value,
                substr(regexp_extract(wsr.sku_value,"[[:digit:]][[:alpha:]]+",0),2) as unit
            from jolly.who_sku_relation as wsr
            left join dim.dim_goods as dg
            on wsr.goods_id = dg.goods_id
            where dg.cate_level1_name = "Kid's Clothing"
        ) as values0
        group by 1,3
    ) as val
    on base.goods_id = val.goods_id
    left join(
        select
            dgp.goods_id,
            dgp.pattern_key_name,
            dgp.pattern_value
        from dim.dim_goods_pattern as dgp
        left join dim.dim_goods as dg
        on dgp.goods_id = dg.goods_id
        where dg.cate_level1_name = "Kid's Clothing"
        and dgp.pattern_key_name="Applicable Gender"
        group by 1,2,3
    ) as gender
    on base.goods_id = gender.goods_id
)

select
    cate_level1_name,
    cate_level2_name,
    cate_level3_name,
    cate_level4_name,
    goods_id,
    goods_name,
    value,
    unit,
    pattern_value,
    case when unit = 'cm' and value >= 120 and pattern_value = 'Boy' then '男大童'
        when unit = 'cm' and value >= 120 and pattern_value = 'Girl' then '女大童'
        when unit = 'cm' and value >= 120 and pattern_value = 'Unisex' then '女大童'
        when unit = 'cm' and value >= 120 and pattern_value = 'Boy' and pattern_value = 'Girl' then '男大童'
        when unit = 'cm' and value >= 120 and pattern_value = 'Boy' and pattern_value = 'Unisex' then '男大童'
        when unit = 'cm' and value >= 120 and pattern_value = 'Girl' and pattern_value = 'Unisex' then '男大童'
        when unit = 'cm' and value >= 120 and pattern_value = 'Boy' and pattern_value = 'Girl' and pattern_value = 'Unisex' then '男大童'

        when unit = 'cm' and value < 120 and value>=80 and pattern_value = 'Boy' then '男小童'
        when unit = 'cm' and value < 120 and value>=80 and pattern_value = 'Girl' then '女小童'
        when unit = 'cm' and value < 120 and value>=80 and pattern_value = 'Unisex' then '男小童'
        when unit = 'cm' and value < 120 and value>=80 and pattern_value = 'Boy' and pattern_value = 'Girl' then '男小童'
        when unit = 'cm' and value < 120 and value>=80 and pattern_value = 'Boy' and pattern_value = 'Unisex' then '女小童'
        when unit = 'cm' and value < 120 and value>=80 and pattern_value = 'Girl' and pattern_value = 'Unisex' then '女小童'
        when unit = 'cm' and value < 120 and value>=80 and pattern_value = 'Boy' and pattern_value = 'Girl' and pattern_value = 'Unisex' then '女小童'

        when unit = 'cm' and value <80 and pattern_value = 'Boy' then '男婴'
        when unit = 'cm' and value <80 and pattern_value = 'Girl' then '女婴'
        when unit = 'cm' and value <80 and pattern_value = 'Unisex' then '婴儿通用'
        when unit = 'cm' and value <80 and pattern_value = 'Boy' and pattern_value = 'Girl' then '婴儿通用'
        when unit = 'cm' and value <80 and pattern_value = 'Boy' and pattern_value = 'Unisex' then '婴儿通用'
        when unit = 'cm' and value <80 and pattern_value = 'Girl' and pattern_value = 'Unisex' then '婴儿通用'
        when unit = 'cm' and value <80 and pattern_value = 'Boy' and pattern_value = 'Girl' and pattern_value = 'Unisex' then '婴儿通用'
        else null end as type
from goods_base
"""


price_1688 = """
with goods_relation as (
    SELECT a1.goods_id,
           CAST(a1.source_goods_id AS BIGINT) AS goods_id_jc
    FROM dw.dw_competing_goods_relation a1
    WHERE a1.data_date = '20191119'
    AND   a1.site_name = 'alibabasearch'
    AND   a1.source_site_name LIKE 'jolly%'
    GROUP BY a1.goods_id,
             CAST(a1.source_goods_id AS BIGINT)
),

competing_goods_info AS (
    SELECT goods_id,
        main_img_url,
        link,
        a1.shop_price,
        a1.promote_price,
        a1.currency,
        ROW_NUMBER() OVER (PARTITION BY a1.goods_id ORDER BY a1.update_date DESC) AS rn
    FROM ods.ods_competing_goods a1
    WHERE a1.site_name IN ('alibaba','alibabasearch')
    AND   a1.data_date = '20191119'
    AND   a1.goods_status = 'online'
    AND   a1.rn = 1
)


select
    a1.category_group,
    case when a1.cate_level1_name = "Beauty" then a1.cate_level2_name else a1.cate_level1_name end as cate_level1_name,
    a1.provider_code,
    supp.supp_name,
    a1.goods_id,
    a2.in_price,
    a4.shop_price,
    a4.promote_price
from dim.dim_goods a1
left join dim.dim_goods_extend as a2
on a1.goods_id = a2.goods_id
and a2.data_date =  '20191119'
LEFT JOIN jolly.who_goods a3 ON a1.goods_id = a3.goods_id
left join zybiro.bi_elaine_supp_name as supp
on a1.provider_code=supp.code
left join(
    SELECT
        a4.goods_id_jc,
        min(a5.shop_price) as shop_price,
        min(a5.promote_price) as promote_price
    FROM goods_relation a4
    LEFT JOIN competing_goods_info a5
         ON a4.goods_id = a5.goods_id
        AND a5.rn = 1
    where a5.goods_id is not null
    group by 1
) as a4
on a1.goods_id = a4.goods_id_jc
where a1.cate_level1_name in ("Event and Party Supplies","Hardwares","Home Decor","Home Linens","Home Supplies","Kitchen & Dining",
            "Pet Supplies","Baby Appliance","Baby Products","Maternity Clothing","Mom Products","Family Matching Outfits",
            "Kid's Accessories","Kid's Bags","Kid's Clothing","Kid's Shoes","Stationery","Toys","Beauty")
and a1.supplier_genre <> 11
and a2.is_jc_on_sale=1
"""

price_1688_msg = """
with goods_relation as (
    SELECT a1.goods_id,
           CAST(a1.source_goods_id AS BIGINT) AS goods_id_jc
    FROM dw.dw_competing_goods_relation a1
    WHERE a1.data_date = '20191119'
    AND   a1.site_name = 'alibabasearch'
    AND   a1.source_site_name LIKE 'jolly%'
    GROUP BY a1.goods_id,
             CAST(a1.source_goods_id AS BIGINT)
),

competing_goods_info AS (
    SELECT goods_id,
        main_img_url,
        link,
        a1.shop_price,
        a1.promote_price,
        a1.currency,
        ROW_NUMBER() OVER (PARTITION BY a1.goods_id ORDER BY a1.update_date DESC) AS rn
    FROM ods.ods_competing_goods a1
    WHERE a1.site_name IN ('alibaba','alibabasearch')
    AND   a1.data_date = '20191119'
    AND   a1.goods_status = 'online'
    AND   a1.rn = 1
)



select
    a1.category_group,
    case when a1.cate_level1_name = "Beauty" then a1.cate_level2_name else a1.cate_level1_name end as cate_level1_name,
    a1.provider_code,
    supp.supp_name,
    a1.goods_id,
    a2.in_price,
    a4.shop_price,
    a4.promote_price,
    a4.link,
    a4.main_img_url
from dim.dim_goods a1
left join dim.dim_goods_extend as a2
on a1.goods_id = a2.goods_id
and a2.data_date =  '20191119'
LEFT JOIN jolly.who_goods a3 ON a1.goods_id = a3.goods_id
left join zybiro.bi_elaine_supp_name as supp
on a1.provider_code=supp.code
left join(
    select
        goods_id_jc,
        main_img_url,
        link,
        shop_price,
        promote_price
    from(
        SELECT
            a4.goods_id_jc,
            a5.main_img_url,
            a5.link,
            a5.shop_price,
            a5.promote_price,
            ROW_NUMBER() OVER (PARTITION BY a4.goods_id_jc ORDER BY a5.promote_price) AS rn
        FROM goods_relation a4
        LEFT JOIN competing_goods_info a5
             ON a4.goods_id = a5.goods_id
            AND a5.rn = 1
        where a5.goods_id is not null
    ) as ss
    where rn =1
) as a4
on a1.goods_id = a4.goods_id_jc
where a1.cate_level1_name in ("Event and Party Supplies","Hardwares","Home Decor","Home Linens","Home Supplies","Kitchen & Dining",
            "Pet Supplies","Baby Appliance","Baby Products","Maternity Clothing","Mom Products","Family Matching Outfits",
            "Kid's Accessories","Kid's Bags","Kid's Clothing","Kid's Shoes","Stationery","Toys","Beauty")
and a1.supplier_genre <> 11
and a2.is_jc_on_sale=1
"""


stock_data_msg = """
with stocks as (
    select
        st.goods_id,
        st.data_date,
        sum(st.total_stock_num) as total_stock_num,
        sum(st.total_stock_num - st.total_order_lock_num - st.total_return_lock_num - st.total_allocate_lock_num) as free_stock_num,
        sum(case when st.depot_id = 15 then st.total_stock_num else 0 end) as uae_tot_stock_num,
        sum(case when st.depot_id = 16 then st.total_stock_num else 0 end) as sa_tot_stock_num,
        sum(case when st.depot_id not in(15,16) then st.total_stock_num else 0 end) as depot_tot_stock_num,
        
        sum(case when st.depot_id = 15 then st.free_stock_num else 0 end) as uae_tot_stock_num_free,
        sum(case when st.depot_id = 16 then st.free_stock_num else 0 end) as sa_tot_stock_num_free,
        sum(case when st.depot_id not in(15,16) then st.free_stock_num else 0 end) as depot_tot_stock_num_free
    from ods.ods_who_wms_goods_stock_total_detail st
    left join dim.dim_goods as dg
    on st.goods_id = dg.goods_id
    left join dim.dim_goods_category_group_new as cat
    on dg.cate_level1_name = cat.cate_level1_name
    where cat.category_group in ('家居','beauty','孕婴童用品','婴童时尚')
    and st.data_date = from_timestamp(date_sub(now(),1),"yyyyMMdd")
    group by 1,2
),

goods_msg as (
    select
        cat.category_group
        ,dg.cate_level1_name
        ,dg.cate_level2_name
        ,dg.cate_level3_name
        ,dg.cate_level4_name
        ,dg.goods_id
        ,dg.goods_sn
        ,dg.goods_name
        ,dg.provider_code
        ,supp.supp_name
        ,brand.brand_name
    from dim.dim_goods as dg
    left join zybiro.bi_elaine_supp_name as supp
    on dg.provider_code = supp.code
    left join jolly.who_brand as brand
    on dg.brand_id = brand.brand_id
    left join dim.dim_goods_extend as dge
    on dg.goods_id = dge.goods_id
    and dge.data_date = from_timestamp(date_sub(now(),1),"yyyyMMdd")
    left join dim.dim_goods_category_group_new as cat
    on dg.cate_level1_name = cat.cate_level1_name
    where cat.category_group in ('家居','beauty','孕婴童用品','婴童时尚')
    and dg.supplier_genre <> 11
    and dge.is_jc_on_sale=1
    and dge.is_jc_sale=1
),

unsold as (
    select
        a.goods_id
        ,age_zx_sa
        ,age_zx_uae
    from zybiro.bi_chili_syb_goods_all a
    inner join dim.dim_goods g on a.goods_id=g.goods_id
    left join dim.dim_goods_category_group_new as cat
    on g.cate_level1_name = cat.cate_level1_name
    where ds=from_timestamp(date_sub(now(),1),"yyyyMMdd")
    and cat.category_group in ('家居','beauty','孕婴童用品','婴童时尚')
),

onway as (
    select
        goods_id,
        sum(case when depot_id=15 then onway else 0 end) as uae_onway,
        sum(case when depot_id=16 then onway else 0 end) as sa_onway,
        sum(case when depot_id in (15,16) then onway else 0 end) as depot_onway
    from(
        select
            skus.goods_id,
            sku_onway.depot_id,
            sum(allocate_order_onway_num)+sum(pur_shiped_order_onway_num) as onway
        from ods.ods_wms_goods_stock_onway_total as sku_onway
        left join ods.ods_who_sku_relation as skus
        on sku_onway.rec_id = skus.rec_id
        and cast(sku_onway.data_date as string) = skus.data_date
        where skus.data_date = from_timestamp(date_sub(now(),1),"yyyyMMdd")
        group by 1,2
    ) as a0
    group by 1
),

goods_sales as (
    select
        b.goods_id,
        onsales.onsale_data,
        sum(b.original_goods_number) as sale_num,
        sum(case when a.depod_id = 15 then b.original_goods_number else 0 end) as uae_sale_num,
        sum(case when a.depod_id = 16 then b.original_goods_number else 0 end) as sa_sale_num,
        sum(case when a.depod_id not in (15,16) then b.original_goods_number else 0 end) as depot_sale_num,
        
        sum(b.original_goods_number)/onsales.onsale_data as unit_day_sale,
        sum(case when a.depod_id = 15 then b.original_goods_number else 0 end)/onsales.onsale_data as unit_day_sale_uae,
        sum(case when a.depod_id = 16 then b.original_goods_number else 0 end)/onsales.onsale_data as unit_day_sale_sa,
        sum(case when a.depod_id not in (15,16) then b.original_goods_number else 0 end)/onsales.onsale_data as unit_day_sale_depot
    from dw.dw_order_sub_order_fact as a
    left join dw.dw_order_goods_fact b on a.order_id = b.order_id
    left join dim.dim_goods as dg
    on b.goods_id = dg.goods_id
    left join dim.dim_goods_category_group_new as cat
    on dg.cate_level1_name = cat.cate_level1_name
    left join(
        select
            dg.goods_id,
            sum(dge.is_jc_on_sale) as onsale_data
        from dim.dim_goods as dg
        left join dim.dim_goods_extend as dge
        on dg.goods_id = dge.goods_id
        left join dim.dim_goods_category_group_new as cat
        on dg.cate_level1_name = cat.cate_level1_name
        where dge.data_date >= from_timestamp(date_sub(now(),31),"yyyyMMdd")
        and dge.data_date <= from_timestamp(date_sub(now(),1),'yyyyMMdd')
        and dg.supplier_genre <> 11
        group by 1
    ) as onsales
    on b.goods_id = onsales.goods_id
    where from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >=from_timestamp(date_sub(now(),31),"yyyyMMdd")
    and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') <= from_timestamp(date_sub(now(),1),'yyyyMMdd')
    and cat.category_group in ('家居','母婴','童玩','beauty','婴童时尚','孕婴童用品')
    and dg.supplier_genre<>11
    group by 1,2
)


select
    goods_msg.category_group          as `类目`
    ,goods_msg.cate_level1_name       as `一级`
    ,goods_msg.cate_level2_name       as `二级`
    ,goods_msg.cate_level3_name       as `三级`
    ,goods_msg.cate_level4_name       as `四级`
    ,goods_msg.goods_id               as `goods_id`
    ,goods_msg.goods_sn               as `goods_sn`
    ,goods_msg.goods_name             as `商品名称`
    ,goods_msg.provider_code          as `供应商编码`
    ,goods_msg.supp_name              as `供应商名称`
    ,goods_msg.brand_name             as `品牌名称`
    ,stocks.total_stock_num           as `总库存`
    ,stocks.free_stock_num            as `总自由库存`
    ,goods_sales.sale_num             as `近30天总销售量`
    ,goods_sales.unit_day_sale        as `近30天日均销售`
    ,stocks.sa_tot_stock_num          as `SA库存`
    ,stocks.sa_tot_stock_num_free     as `SA自由库存`
    ,unsold.age_zx_sa                 as `SA滞销等级`
    ,goods_sales.sa_sale_num          as `SA近30天销量`
    ,goods_sales.unit_day_sale_sa     as `SA近30天日均销量`
    ,stocks.sa_tot_stock_num_free/stocks.sa_tot_stock_num_free as `SA库存天数`
    ,onway.sa_onway                   as `SA在途库存`
    ,stocks.uae_tot_stock_num         as `USA总库存`
    ,stocks.uae_tot_stock_num_free    as `UAE自由库存`
    ,unsold.age_zx_uae                as `UAE滞销等级`
    ,goods_sales.uae_sale_num         as `UAE近30天销`
    ,goods_sales.unit_day_sale_uae    as `UAE近30天日均销量`
    ,stocks.uae_tot_stock_num_free/goods_sales.unit_day_sale_uae as `UAE库存天数`
    ,onway.uae_onway                  as `UAE在途库存`
    ,stocks.depot_tot_stock_num       as `国内总库存`
    ,stocks.depot_tot_stock_num_free  as `国内自由库存`
    ,goods_sales.depot_sale_num       as `国内近30天销量`
    ,goods_sales.unit_day_sale_depot  as `国内近30天日均销量`
    ,stocks.depot_tot_stock_num_free/stocks.depot_tot_stock_num_free as `国内库存天数`
    ,onway.depot_onway                as `国内在途库存`
    ,stocks.data_date                 as `日期`
from goods_msg left join stocks
on goods_msg.goods_id = stocks.goods_id
left join unsold
on goods_msg.goods_id = unsold.goods_id
left join goods_sales
on goods_msg.goods_id = goods_sales.goods_id
left join onway
on goods_msg.goods_id = onway.goods_id
order by goods_msg.category_group,
    goods_msg.cate_level1_name,
    goods_sales.sale_num desc;
"""

sku_sale_tmp_msg = """
    select
        cat.category_group,
        case when c.cate_level1_name='Beauty' then c.cate_level2_name else c.cate_level1_name end as cate_level1_name,
        b.goods_id,
        b.sku_id,
        sum(b.original_goods_number) num,
        sum(b.original_goods_number*b.goods_price) revenue,
        sum(b.original_goods_number*b.in_price_usd) +nvl(sum( case when ((lower(a.country_name)='saudi arabia' and e.is_sa_supplier<>1) 
            or  (lower(a.country_name)='united arab emirates' and e.supplier_genre<>10)) then b.goods_price*b.original_goods_number*0.05
            when lower(a.country_name) in ('saudi arabia','united arab emirates')
            then  (b.goods_price-b.in_price_usd)*b.original_goods_number/1.05*0.05 end),0) cost_with_vat
    from dw.dw_order_goods_fact b 
    inner join  dw.dw_order_fact a on a.order_id=b.order_id
    left join dim.dim_goods c on c.goods_id=b.goods_id
    left join jolly.who_esoloo_supplier  e on  c.provider_code = e.code
    left join dim.dim_goods_category_group_new as cat
    on c.cate_level1_name = cat.cate_level1_name
    where
    from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >= from_timestamp(date_sub(now(),31),'yyyyMMdd')
        and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') <= from_timestamp(date_sub(now(),1),'yyyyMMdd')
    and a.site_id  in(400,600,700,601,900) 
    and a.pay_status in(1,3)
    and cat.category_group = '婴童时尚'
    and c.supplier_genre<>11
    group by
    1,2,3,4
"""


stock_goods_sale = """
with sales as (
    select
        a.order_id,
        a.source_order_id,
        from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') data_date,
        case when a.depod_id = 15 then 'UAE'
            when a.depod_id = 16 then 'SA'
            else 'depot' end as depot_type,
        cat.category_group,
        case when dg.cate_level1_name="Beauty" then dg.cate_level2_name else dg.cate_level1_name end as cate_level1_name,
        b.goods_id,
        b.sku_id,
        sum(b.original_goods_number) as num,
        sum(b.original_goods_number*b.goods_price) as gmv
    from dw.dw_order_sub_order_fact as a
    left join dw.dw_order_goods_fact b on a.order_id = b.order_id
    left join dim.dim_goods as dg on b.goods_id = dg.goods_id
    left join dim.dim_goods_category_group_new as cat on dg.cate_level1_name = cat.cate_level1_name
    where from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >='20190501'
    and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') <= '20191031'
    and dg.category_group in ('家居','母婴','童玩','beauty','婴童时尚','孕婴童用品')
    and dg.supplier_genre <> 11
    and b.site_id in (400,600,700,601,900)
    and a.depod_id not in (15,16)
    group by 1,2,3,4,5,6,7,8
),

stocks as (
    select
        cat.category_group,
        stocks.goods_id,
        stocks.sku_id,
        stocks.sale_sku_id,
        case when stocks.depot_id = 15 then 'UAE'
            when stocks.depot_id = 16 then 'SA'
            else 'depot' end depot_type,
        sum(stocks.total_stock_num) as total_stock_num,
        sum(stocks.total_stock_num - stocks.total_order_lock_num - stocks.total_return_lock_num - stocks.total_allocate_lock_num) as free_stock_num,
        stocks.data_date
    from ods.ods_who_wms_goods_stock_total_detail as stocks
    left join dim.dim_goods as dg on stocks.goods_id = dg.goods_id
    left join dim.dim_goods_extend as dge
    on dg.goods_id = dge.goods_id
    and dge.data_date = stocks.data_date
    left join dim.dim_goods_category_group_new as cat
    on dg.cate_level1_name = cat.cate_level1_name
    where stocks.data_date >= '20190501'
    and stocks.data_date <= '20191031'
    and cat.category_group in ('家居','母婴','童玩','beauty','婴童时尚','孕婴童用品')
    and dge.is_jc_on_sale=1
    group by 1,2,3,4,5,8
),

sale_data as (
    select
        data_date,
        depot_type,
        goods_id,
        sku_id,
        sum(num) as num,
        sum(gmv) as gmv
    from sales
--     where source_order_id = 0
    group by 1,2,3,4
),

stock_fbj as (
    select
        category_group,
        goods_id,
        depot_type,
        sum(total_stock_num) as total_stock_num,
        sum(free_stock_num) as free_stock_num,
        data_date_stock,
        sale_depot_type,
        sum(num) as num,
        sum(gmv) as gmv,
        data_date_sale
    from(
        select
            stocks.category_group,
            stocks.goods_id,
            stocks.sku_id,
            stocks.sale_sku_id,
            stocks.depot_type,
            stocks.total_stock_num,
            stocks.free_stock_num,
            stocks.data_date as data_date_stock,
            sd.depot_type as sale_depot_type,
            sd.num,
            sd.gmv,
            sd.data_date as data_date_sale
        from stocks left join sale_data as sd
        on stocks.goods_id = sd.goods_id
        and stocks.sale_sku_id = sd.sku_id
        and from_timestamp(date_add(from_unixtime(UNIX_TIMESTAMP(stocks.data_date,'yyyyMMdd')),1),"yyyyMMdd") = sd.data_date
        and stocks.depot_type = sd.depot_type
    ) as base
    group by 1,2,3,6,7,10
),

stocks_today as (
    select
        cat.category_group,
        dg.cate_level1_name,
        stocks.goods_id,
        sum(case when stocks.depot_id = 15 then stocks.total_stock_num end) as UAE_tot_stock_num,
        sum(case when stocks.depot_id = 16 then stocks.total_stock_num end) as SA_tot_stock_num,
        sum(case when stocks.depot_id in (15,16) then stocks.total_stock_num end) as depot_tot_stock_num,
        sum(case when stocks.depot_id = 15 then stocks.free_stock_num end) as UAE_free_stock_num,
        sum(case when stocks.depot_id = 16 then stocks.free_stock_num end) as SA_free_stock_num,
        sum(case when stocks.depot_id in (15,16) then stocks.free_stock_num end) as depot_free_stock_num
    from ods.ods_who_wms_goods_stock_total_detail as stocks
    left join dim.dim_goods as dg on stocks.goods_id = dg.goods_id
    left join dim.dim_goods_extend as dge
    on dg.goods_id = dge.goods_id
    and dge.data_date = stocks.data_date
    left join dim.dim_goods_category_group_new as cat
    on dg.cate_level1_name = cat.cate_level1_name
    where stocks.data_date = '20191121'
    and cat.category_group in ('家居','母婴','童玩','beauty','婴童时尚','孕婴童用品')
    and dge.is_jc_on_sale=1
    group by 1,2,3
)


select
    st.category_group,
    st.cate_level1_name,
    st.goods_id,
    st.UAE_tot_stock_num,
    st.SA_tot_stock_num,
    st.depot_tot_stock_num,
    st.UAE_free_stock_num,
    st.SA_free_stock_num as SA_free_stock_num_now,
    st.depot_free_stock_num,
    
    base.sa_free_stock_num,
    base.num,
    base.sa_sale_num
from stocks_today as st
left join(
    select
        *
    from(
        select
            goods_id,
            sum(case when depot_type='SA' then free_stock_num else 0 end) as sa_free_stock_num,
            sum(num) as num,
            sum(case when sale_depot_type = "UAE" then num else 0 end) as uae_sale_num,
            sum(case when sale_depot_type = "SA" then num else 0 end) as sa_sale_num
        from stock_fbj
        group by 1
    ) as ss
    where num > 0
    and sa_free_stock_num > 0
) as base
on st.goods_id = base.goods_id
"""


def bu_report_daily(begin: str, end: str, begin_last_year: str, end_last_year: str):
    """
    begin: 开始时间
    end: 截止时间
    begin_last_year: 去年开始时间
    end_last_year: 去年截止时间
    时间为yyyyMMdd格式
    """
    sql_msg = """
        with sales as (
            select
                from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') data_date,
                c.category_group,
                c.cate_level1_name,
                case when c.supplier_genre = 11 then 1 else 0 end as is_pop,
                sum(b.original_goods_number) num,
                sum(b.original_goods_number*b.goods_price) revenue,
                sum(b.original_goods_number*b.in_price_usd) +nvl(sum( case when ((lower(a.country_name)='saudi arabia' and e.is_sa_supplier<>1) 
                    or  (lower(a.country_name)='united arab emirates' and e.supplier_genre<>10)) then b.goods_price*b.original_goods_number*0.05
                    when lower(a.country_name) in ('saudi arabia','united arab emirates')
                    then  (b.goods_price-b.in_price_usd)*b.original_goods_number/1.05*0.05 end),0) cost_with_vat
            from dw.dw_order_goods_fact b 
            inner join  dw.dw_order_fact a on a.order_id=b.order_id
            left join dim.dim_goods c on c.goods_id=b.goods_id
            left join jolly.who_esoloo_supplier  e on  c.provider_code = e.code
            where
            ((from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >='{begin}'
                and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') <= '{end}')
            or (from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >= '{begin_last_year}'
                and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd')<= '{end_last_year}'))
            and a.site_id  in(400,600,700,601,900) 
            and a.pay_status in(1,3)
            group by 1,2,3,4
        ),

        dau as (
            select
                dg.category_group,
                dg.cate_level1_name,
                dau.data_date,
                case when dg.supplier_genre = 11 then 1 else 0 end as is_POP,
                sum(dau.expo_pv) as expo_pv,
                sum(dau.expo_uv) as expo_uv,
                sum(dau.goods_click_uv) as goods_click_uv,
                sum(dau.bill_uv) as bill_uv,
                sum(dau.bill_uv)/sum(dau.goods_click_uv) as click_bill,
                sum(dau.sales_uv) as sales_uv,
                sum(dau.sales_uv)/sum(dau.goods_click_uv) as click_sale
            from rpt.rpt_sum_goods_daily as dau
            left join dim.dim_goods as dg
            on dau.goods_id = dg.goods_id
            where dau.data_date >= from_timestamp(months_sub(trunc(now(), "MM"), 1), "yyyyMMdd")
            and dau.data_date <= from_timestamp(date_sub(now(),1), "yyyyMMdd")
            and dau.site_id in (400,600,700,601,900)
            group by 1,2,3,4
        ),

        base as (
            select
                sales.category_group,
                sales.cate_level1_name,
                sales.data_date,
                sales.is_pop,
                sales.num,
                sales.revenue,
                sales.cost_with_vat,
                dau.expo_pv,
                dau.expo_uv,
                dau.goods_click_uv,
                dau.bill_uv,
                dau.sales_uv
            from sales left join dau
            on sales.category_group = dau.category_group
            and sales.cate_level1_name = dau.cate_level1_name
            and sales.data_date = dau.data_date
            and sales.is_pop = dau.is_pop
        )


        select
            case when cat.category_group in ('家居','婚庆周边')                                             then '家居'
                when cat.category_group in ('婴童时尚','孕婴童用品')                                        then '母婴童'
                when cat.category_group in ('女装','服装2（内衣）','鞋包','ACC','男装',"包", "鞋",'Sports') then '时尚'
                when cat.category_group in ('数码', '车品','Appliances','Computers & Accessories')          then '3C'
                when cat.category_group in ('手机')                                                         then '手机'
                when cat.category_group in ('beauty')                                                       then '美妆' 
                when cat.category_group in ('快消')                                                         then '执御超市'
                else '其他' end                                                                              as `BU`
            ,base.cate_level1_name
            ,data_date
            ,is_pop
            ,substr(data_date,1,4) as years0
            ,round(sum(revenue),2)                                                                as `GMV`
            ,round(sum(cost_with_vat),2)                                                          as `成本`
            ,(sum(revenue) - sum(cost_with_vat))/sum(revenue)                                     as `毛利率`
            ,sum(base.expo_uv)                                                                    as `曝光uv`
            ,sum(base.expo_pv)                                                                    as `曝光pv`
            ,sum(base.goods_click_uv)                                                             as `点击uv`
            ,sum(base.bill_uv)                                                                    as `下单uv`
            ,sum(base.sales_uv)                                                                   as `购买uv`
        from base
        left join dim.dim_goods_category_group_new as cat
        on base.cate_level1_name = cat.cate_level1_name
        where ((data_date >= '{begin}'
          and data_date <= '{end}')
        or (data_date >= '{begin_last_year}'
          and data_date <= '{end_last_year}'))
        and cat.category_group != '快消'
        group by 1,2,3,4
        order by case when cat.category_group in ('家居','婚庆周边')                                        then '家居'
                when cat.category_group in ('婴童时尚','孕婴童用品')                                        then '母婴童'
                when cat.category_group in ('女装','服装2（内衣）','鞋包','ACC','男装',"包", "鞋",'Sports') then '时尚'
                when cat.category_group in ('数码', '车品','Appliances','Computers & Accessories')          then '3C'
                when cat.category_group in ('手机')                                                         then '手机'
                when cat.category_group in ('beauty')                                                       then '美妆' 
                when cat.category_group in ('快消')                                                         then '执御超市'
                else '其他' end
            ,base.cate_level1_name
            ,data_date
        """.format(begin=begin, end=end, begin_last_year=begin_last_year, end_last_year=end_last_year)
    return sql_msg


def goods_stock_week_report():
    """商品库存周报"""
    sql_msg = """
    select
        sga.category_group_1 as `类目`,
        case when sga.category_group_1 = 'beauty' then cat_level2_name else cat_level1_name end as `一级`,
        case when sga.category_group_1 = 'beauty' then cat_level3_name else cat_level2_name end as `二级`,
        case when sga.category_group_1 = 'beauty' then cat_level4_name else cat_level3_name end as `三级`,
        sga.goods_id as `id`,
        sga.goods_name as `商品名称`,
        sga.goods_season as `季节`,
        sga.first_on_sale_time as `上架时间`,
        sga.in_price as `成本(￥)`,
        sga.in_price_usd as `成本($)`,
        sga.shop_price_1 as `原价`,
        sga.prst_price_1 as `促销价`,
        sga.stock as `总库存`,
        sga.stock_sa as `sa总库存`,
        sga.stock_uae as `uae总库存`,
        sga.free_stock as `自由库存`,
        sga.free_stock_sa as `sa自由库存`,
        sga.free_stock_uae as `uae自由库存`,
        sga.onway as `在途库存`,
        sga.age_zx_sa as `滞销等级sa`,
        sga.age_zx_uae as `滞销等级uae`,
        sga.goods_num_30 as `近30天销量`,
        sga.goods_revenue_30 as `近30天GMV`,
        sga.goods_revenue_30_m as `近30天毛利率`,
        sga.goods_num_30/sga.free_stock as `可销天数`,
        sga.goods_reason_return_180d as `退货件数-180天`,
        sga.goods_reason_return_rate_180d as `退货率-180天`,
        sga.goods_reason_return_30d as `退货件数-30天`,
        sga.goods_reason_return_rate_30d as `退货率-30天`,
        sga.bad_comments_rate as `差评率-180天`
    from zybiro.bi_chili_syb_goods_all as sga
    where ds = from_timestamp(date_sub(now(),1), "yyyyMMdd")
    and category_group_1 in ('beauty','家居','婴童时尚','孕婴童用品')
    order by sga.category_group_1, 
        case when sga.category_group_1 = 'beauty' then cat_level2_name else cat_level1_name end,
        case when sga.category_group_1 = 'beauty' then cat_level3_name else cat_level2_name end,
        case when sga.category_group_1 = 'beauty' then cat_level4_name else cat_level3_name end
    """
    return sql_msg


def goods_sale_rate_week():
    sql_msg = """
    with sales as (
        select
            a0.category_group,
            a0.cate_level1_name,
            a0.cate_level2_name,
            a0.cate_level3_name,
            case when a0.data_date > to_date(date_sub(now(),7*1+1)) and a0.data_date <= to_date(date_sub(now(),0*7+1)) then '前1周'
                when a0.data_date > to_date(date_sub(now(), 7*2+1)) and a0.data_date <= to_date(date_sub(now(),1*7+1)) then '前2周'
                when a0.data_date > to_date(date_sub(now(), 7*3+1)) and a0.data_date <= to_date(date_sub(now(),2*7+1)) then '前3周'
                when a0.data_date > to_date(date_sub(now(), 7*4+1)) and a0.data_date <= to_date(date_sub(now(),3*7+1)) then '前4周'
                when a0.data_date > to_date(date_sub(now(), 7*5+1)) and a0.data_date <= to_date(date_sub(now(),4*7+1)) then '前5周'
                when a0.data_date > to_date(date_sub(now(), 7*6+1)) and a0.data_date <= to_date(date_sub(now(),5*7+1)) then '前6周'
                when a0.data_date > to_date(date_sub(now(), 7*7+1)) and a0.data_date <= to_date(date_sub(now(),6*7+1)) then '前7周'
                when a0.data_date > to_date(date_sub(now(), 7*8+1)) and a0.data_date <= to_date(date_sub(now(),7*7+1)) then '前8周'
            else null end as `weeks0`,
            count(distinct a0.goods_id) as sale_goods_num
        from(
            select
                c.category_group,
                case when c.cate_level1_name = "Beauty" then c.cate_level2_name else c.cate_level1_name end as cate_level1_name,
                case when c.cate_level1_name = "Beauty" then c.cate_level3_name else c.cate_level2_name end as cate_level2_name,
                case when c.cate_level1_name = "Beauty" then c.cate_level4_name else c.cate_level3_name end as cate_level3_name,
                to_date(case when a.pay_id=41 then a.pay_time else a.result_pay_time end) data_date,
                b.goods_id,
                sum(b.original_goods_number) num,
                sum(b.original_goods_number*b.goods_price) revenue
            from dw.dw_order_goods_fact b 
            inner join  dw.dw_order_fact a on a.order_id=b.order_id
            left join dim.dim_goods c on c.goods_id=b.goods_id
            left join jolly.who_esoloo_supplier  e on  c.provider_code = e.code
            where
            from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >= from_timestamp(date_sub(now(), 56), "yyyyMMdd")
            and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') <= from_timestamp(date_sub(now(), 1), "yyyyMMdd")
            and a.site_id  in(400,600,700,601,900) 
            and a.pay_status in(1,3)
            and c.supplier_genre <> 11
            group by 1,2,3,4,5,6
        ) as a0
        group by 1,2,3,4,5
    ),
    
    goods as (
        select
            a0.category_group,
            a0.cate_level1_name,
            a0.cate_level2_name,
            a0.cate_level3_name,
            case when a0.data_date > to_date(date_sub(now(),7*1+1)) and a0.data_date<=to_date(date_sub(now(),0*7+1)) then '前1周'
                when a0.data_date > to_date(date_sub(now(),7*2+1)) and a0.data_date<=to_date(date_sub(now(),1*7+1)) then '前2周'
                when a0.data_date > to_date(date_sub(now(),7*3+1)) and a0.data_date<=to_date(date_sub(now(),2*7+1)) then '前3周'
                when a0.data_date > to_date(date_sub(now(),7*4+1)) and a0.data_date<=to_date(date_sub(now(),3*7+1)) then '前4周'
                when a0.data_date > to_date(date_sub(now(),7*5+1)) and a0.data_date<=to_date(date_sub(now(),4*7+1)) then '前5周'
                when a0.data_date > to_date(date_sub(now(),7*6+1)) and a0.data_date<=to_date(date_sub(now(),5*7+1)) then '前6周'
                when a0.data_date > to_date(date_sub(now(),7*7+1)) and a0.data_date<=to_date(date_sub(now(),6*7+1)) then '前7周'
                when a0.data_date > to_date(date_sub(now(),7*8+1)) and a0.data_date<=to_date(date_sub(now(),7*7+1)) then '前8周'
                else null end as `weeks0`,
            count(distinct a0.goods_id) as goods_num
        from(
            select
                dg.category_group,
                case when dg.cate_level1_name = "Beauty" then dg.cate_level2_name else dg.cate_level1_name end as cate_level1_name,
                case when dg.cate_level1_name = "Beauty" then dg.cate_level3_name else dg.cate_level2_name end as cate_level2_name,
                case when dg.cate_level1_name = "Beauty" then dg.cate_level4_name else dg.cate_level3_name end as cate_level3_name,
                from_unixtime(unix_timestamp(dge.data_date, "yyyyMMdd"), "yyyy-MM-dd") as data_date,
                dg.goods_id
            from dim.dim_goods as dg
            left join dim.dim_goods_extend as dge
            on dg.goods_id = dge.goods_id
            and dge.data_date >= from_timestamp(date_sub(now(), 56), "yyyyMMdd")
            and dge.data_date <= from_timestamp(date_sub(now(), 1), "yyyyMMdd")
            left join jolly.who_esoloo_supplier as supp
            on dg.provider_code = supp.code
            left join zybiro.bi_elaine_supp_name as supp_name
            on supp.code = supp_name.code
            where dge.is_jc_on_sale = 1
            and dg.category_group in ('beauty', '家居','婴童时尚','孕婴童用品')
            and dg.supplier_genre <> 11
        ) as a0
        group by 1,2,3,4,5
    )
    
    select
        goods.category_group as `类目`,
        goods.cate_level1_name as `一级`,
        goods.cate_level2_name as `二级`,
        goods.cate_level3_name as `三级`,
        goods.weeks0 as `周`,
        goods.goods_num as `在架款数`,
        sales.sale_goods_num as `销售款数`
    from goods left join sales
    on goods.category_group = sales.category_group
    and goods.cate_level1_name = sales.cate_level1_name
    and goods.cate_level2_name = sales.cate_level2_name
    and goods.cate_level3_name = sales.cate_level3_name
    and goods.weeks0 = sales.weeks0
    """
    return sql_msg


def user_resale_second_week():
    """用户复购-二级周报/月报"""
    sqlmsg = """
    with sale_info as 
    (
        select 
            *
            ,row_number () over (PARTITION BY user_id,category_group,cate_level1_name ORDER BY data_date ) order_num
        from 
        (
            select
                from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd')  data_date
                ,user_id
                ,c.category_group
                ,case when c.cate_level1_name='Beauty' then c.cate_level2_name else c.cate_level1_name end as cate_level1_name
                ,case when c.cate_level1_name='Beauty' then c.cate_level3_name else c.cate_level2_name end as cate_level2_name
                ,sum(b.original_goods_number) num
                ,sum(b.original_goods_number*b.goods_price) revenue 
            from    
            dw.dw_order_goods_fact b inner join  dw.dw_order_fact a on a.order_id=b.order_id
            left join dim.dim_goods c on c.goods_id=b.goods_id
            left join jolly.who_esoloo_supplier  e on  c.provider_code = e.code
            where
            from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >= from_timestamp(trunc(months_sub(now(), 11), "MM"), "yyyyMMdd")
            and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') < from_timestamp(trunc(now(), "MM"), "yyyyMMdd")
            and a.site_id  in(400,600,700,601,900) 
            and a.pay_status in(1,3)
            and c.category_group in ('家居','母婴','童玩','beauty')
            and c.supplier_genre<>11 
            group by 
            1,2,3,4,5
        )  ws
        order by 
        user_id
        ,category_group
        ,cate_level1_name
        ,cate_level2_name
    )
    --------part1到类目组维度的复购情况 
    select 
        category_group
        ,cate_level1_name
        ,cate_level2_name
        ,count(case when order_num =1 then 1 else null end ) as `购买人数_1次`
        ,count(case when order_num =2 then 1 else null end ) as `购买人数_2次`
        ,count(case when order_num =3 then 1 else null end ) as `购买人数_3次`
        ,count(case when order_num =4 then 1 else null end ) as `购买人数_4次`
        ,sum(case when  order_num =1  then revenue  else 0 end ) as `GMV_1次`
        ,sum(case when  order_num =2  then revenue  else 0 end ) as `GMV_2次`
        ,sum(case when  order_num =3  then revenue  else 0 end ) as `GMV_3次`
        ,sum(case when  order_num =4  then revenue  else 0 end ) as `GMV_4次`
        ,sum(case when  order_num =1  then revenue  else 0 end )/count(case when order_num =1 then 1 else null end ) as `客单价_1次`
        ,sum(case when  order_num =2  then revenue  else 0 end )/count(case when order_num =2 then 1 else null end ) as `客单价_2次`
        ,sum(case when  order_num =3  then revenue  else 0 end )/count(case when order_num =3 then 1 else null end ) as `客单价_3次`
        ,sum(case when  order_num =4  then revenue  else 0 end )/count(case when order_num =4 then 1 else null end ) as `客单价_4次`
        ,'/' as `流失率_1次`
        ,count(case when order_num =2 then 1 else null end )/count(case when order_num =1 then 1 else null end ) as `流失率_2次`
        ,count(case when order_num =3 then 1 else null end )/count(case when order_num =2 then 1 else null end ) as `流失率_3次`
        ,count(case when order_num =4 then 1 else null end )/count(case when order_num =3 then 1 else null end ) as `流失率_4次`
    from 
    sale_info
    where  
    user_id in  (---用户首单的时间
        select 
        distinct user_id
        from sale_info 
        where data_date <= from_timestamp(trunc(months_sub(now(),6), "MM"), "yyyyMMdd")
    )
    group by category_group
    ,cate_level1_name
    ,cate_level2_name
    order by category_group
    ,cate_level1_name
    ,cate_level2_name
    """
    return sqlmsg


def user_resale_first_week():
    """用户复购-一级周报/月报"""
    sqlmsg = """
    with sale_info as 
    (
        select 
            *
            ,row_number () over (PARTITION BY user_id,category_group ORDER BY data_date ) order_num
        from 
        (
            select
                from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd')  data_date
                ,user_id
                ,c.category_group
                ,case when c.cate_level1_name='Beauty' then c.cate_level2_name else c.cate_level1_name end as cate_level1_name
                ,sum(b.original_goods_number) num
                ,sum(b.original_goods_number*b.goods_price) revenue 
            from    
            dw.dw_order_goods_fact b inner join  dw.dw_order_fact a on a.order_id=b.order_id
            left join dim.dim_goods c on c.goods_id=b.goods_id
            left join jolly.who_esoloo_supplier  e on  c.provider_code = e.code
            where
            from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >= from_timestamp(trunc(months_sub(now(), 11), "MM"), "yyyyMMdd")
            and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') < from_timestamp(trunc(now(), "MM"), "yyyyMMdd")
            and a.site_id  in(400,600,700,601,900) 
            and a.pay_status in(1,3)
            and c.category_group in ('家居','母婴','童玩','beauty')
            and c.supplier_genre<>11 
            group by 
            1,2,3,4
        )  ws
        order by 
        user_id
        ,category_group
        ,cate_level1_name
    )
    --------part1到类目组维度的复购情况 
    select 
        category_group
        ,cate_level1_name
        ,count(case when order_num =1 then 1 else null end ) as `购买人数_1次`
        ,count(case when order_num =2 then 1 else null end ) as `购买人数_2次`
        ,count(case when order_num =3 then 1 else null end ) as `购买人数_3次`
        ,count(case when order_num =4 then 1 else null end ) as `购买人数_4次`
        ,sum(case when  order_num =1  then revenue  else 0 end ) as `GMV_1次`
        ,sum(case when  order_num =2  then revenue  else 0 end ) as `GMV_2次`
        ,sum(case when  order_num =3  then revenue  else 0 end ) as `GMV_3次`
        ,sum(case when  order_num =4  then revenue  else 0 end ) as `GMV_4次`
        ,sum(case when  order_num =1  then revenue  else 0 end )/count(case when order_num =1 then 1 else null end ) as `客单价_1次`
        ,sum(case when  order_num =2  then revenue  else 0 end )/count(case when order_num =2 then 1 else null end ) as `客单价_2次`
        ,sum(case when  order_num =3  then revenue  else 0 end )/count(case when order_num =3 then 1 else null end ) as `客单价_3次`
        ,sum(case when  order_num =4  then revenue  else 0 end )/count(case when order_num =4 then 1 else null end ) as `客单价_4次`
        ,'/' as `流失率_1次`
        ,count(case when order_num =2 then 1 else null end )/count(case when order_num =1 then 1 else null end ) as `流失率_2次`
        ,count(case when order_num =3 then 1 else null end )/count(case when order_num =2 then 1 else null end ) as `流失率_3次`
        ,count(case when order_num =4 then 1 else null end )/count(case when order_num =3 then 1 else null end ) as `流失率_4次`
    from 
    sale_info
    where  
    user_id in  (---用户首单的时间
        select 
        distinct user_id
        from sale_info 
        where data_date <= from_timestamp(trunc(months_sub(now(),6), "MM"), "yyyyMMdd")
    )
    group by category_group
    ,cate_level1_name
    order by category_group
    ,cate_level1_name"""
    return sqlmsg


def cate_level1_month():
    sqlmsg = """
    with sales as (
        select
            from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMM') year_month,
            from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyy') as years0,
            cat.category_group,
            case when c.cate_level1_name='Beauty' then c.cate_level2_name else c.cate_level1_name end as cate_level1_name,
            sum(b.original_goods_number*b.goods_price) revenue,
            sum(b.original_goods_number) as num,
            sum(b.original_goods_number*b.in_price_usd) +nvl(sum( case when ((lower(a.country_name)='saudi arabia' and e.is_sa_supplier<>1) 
                or  (lower(a.country_name)='united arab emirates' and e.supplier_genre<>10)) then b.goods_price*b.original_goods_number*0.05
                when lower(a.country_name) in ('saudi arabia','united arab emirates')
                then  (b.goods_price-b.in_price_usd)*b.original_goods_number/1.05*0.05 end),0) cost_with_vat
        from dw.dw_order_goods_fact b 
        inner join  dw.dw_order_fact a on a.order_id=b.order_id
        left join dim.dim_goods c on c.goods_id=b.goods_id
        left join jolly.who_esoloo_supplier  e on  c.provider_code = e.code
        left join dim.dim_goods_category_group_new as cat
        on c.cate_level1_name = cat.cate_level1_name
        where
        ((from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >= from_timestamp(trunc(now(),'Y'),'yyyyMMdd')
            and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') <= from_timestamp(date_sub(now(),1),'yyyyMMdd') )
        or (from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >= from_timestamp(years_sub(trunc(now(),'Y'),1),'yyyyMMdd')
        and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') <= from_timestamp(years_sub(date_sub(now(),1),1),'yyyyMMdd') ))
        and a.site_id  in(400,600,700,601,900) 
        and a.pay_status in(1,3)
        and cat.category_group in ('家居','母婴','童玩','beauty','婴童时尚','孕婴童用品')
        and c.supplier_genre<>11
        group by 1,2,3,4
    ),

    goods as (
        select
            cat.category_group,
            case when dg.cate_level1_name = "Beauty" then dg.cate_level2_name else dg.cate_level1_name end as cate_level1_name,
            substr(dge.data_date,1,6) as year_month,
            substr(dge.data_date,1,4) as years0,
            count(distinct dg.goods_id) as goods_num
        from dim.dim_goods as dg
        left join dim.dim_goods_extend as dge
        on dg.goods_id = dge.goods_id
        and dge.data_date >= from_timestamp(years_sub(trunc(now(), "YY"),1), "yyyyMMdd")
        and dge.data_date <= from_timestamp(date_sub(now(), 1), "yyyyMMdd")
        left join jolly.who_esoloo_supplier as supp
        on dg.provider_code = supp.code
        left join zybiro.bi_elaine_supp_name as supp_name
        on supp.code = supp_name.code
        left join dim.dim_goods_category_group_new as cat
        on dg.cate_level1_name = cat.cate_level1_name
        where dge.is_jc_on_sale = 1
        and cat.category_group in ('beauty', '家居','婴童时尚','孕婴童用品')
        and dg.supplier_genre <> 11
        group by 1,2,3,4
    )


    select
        goods.category_group,
        goods.cate_level1_name,
        goods.year_month,
        goods.years0,
        goods.goods_num,
        sales.revenue,
        sales.num,
        sales.cost_with_vat
    from goods left join sales
    on goods.category_group = sales.category_group
    and goods.cate_level1_name = sales.cate_level1_name
    and goods.year_month = sales.year_month
    """
    return sqlmsg


def new_supp_week():
    sqlmsg = """
    with supp as (
        select 
            ws.code ,
            ws1.supp_name,
            from_unixtime(ws.great_time) as great_time,
            case when ws.is_hide = 1 then '取消合作'
                    when ws.pre_cancel = 1 then '预取消'
                    else '合作中' end 'cooperate_status' ,
            case when ws.supplier_nature = 1 then '自有工厂'
                     when ws.supplier_nature = 2 then '经销商'
                     when ws.supplier_nature = 3 then 'OEM贴牌'
                     else '' end supp_type,
            ws3.user_name operation_admin_id, --运营
            ws4.user_name buyer_admin_id, -- 买手
            ws5.user_name bd_admin_id, --BD
            ws6.user_name admin_id --采购
        from  jolly.who_esoloo_supplier ws
        left join (select distinct code,supp_name from zybiro.bi_elaine_supp_name)  ws1 on ws.code = ws1.code
        left join jolly.who_rbac_user ws3  on ws.operation_admin_id = ws3.user_id
        left join jolly.who_rbac_user ws4 on ws.buyer_admin_id = ws4.user_id
        left join jolly.who_rbac_user ws5 on ws.bd_admin_id = ws5.user_id
        left join jolly.who_rbac_user ws6 on ws.admin_id = ws6.user_id
        where from_unixtime(ws.great_time) >= from_timestamp(date_sub(now(),7),'yyyy-MM-dd')
        and ws.supplier_genre != 11
        and ws4.user_name in ("胡亚芳","方铭婧","徐婷","薛玉","李国强","魏茜茜",
                              "周倩南","楚玉","冀菲","方淑芬","徐璐雨")
        and ws.is_hide != 1
        and ws.pre_cancel != 1
    ),
    
    goods as (
        select
            dg.provider_code,
            count(distinct dg.goods_id) as goods_num
        from dim.dim_goods as dg
        left join dim.dim_goods_extend as dge
        on dg.goods_id = dge.goods_id
        and dge.data_date = from_timestamp(date_sub(now(), 1), "yyyyMMdd")
        left join jolly.who_esoloo_supplier as supp
        on dg.provider_code = supp.code
        left join zybiro.bi_elaine_supp_name as supp_name
        on supp.code = supp_name.code
        where dg.category_group in ('beauty', '家居','婴童时尚','孕婴童用品')
        and dg.supplier_genre <> 11
        group by 1
    )
    
    select
        supp.code as `编码`,
        supp.supp_name as `供应商名称`,
        supp.great_time as `合作时间`,
        supp.cooperate_status as `合作状态`,
        supp.supp_type as `供应商类型`,
        supp.operation_admin_id as `运营`,
        supp.buyer_admin_id as `买手`,
        supp.bd_admin_id as `BD`,
        supp.admin_id as `采购`,
        goods.goods_num as `上新数量`
    from supp left join goods
    on supp.code = goods.provider_code
    """
    return sqlmsg


def supp_week_sql(begin, end):
    sqlmsg = """
    with base as (
        select
            base.supp_id,
            base.code,
            supp_name.supp_name,
            base.status,
            base.provider_type,
            base.great_time,
            on_sales.year_month,
            on_sales.goods_num,
            new_goods_num,
            on_sales.sku_num,
            sales.num,
            sales.sale_goods_num,
            sales.revenue,
            sales.cost_usd,
            sales.cost_rmb,
            sales.cost_with_vat
        from(
            select
                wes.supp_id,
                wes.code,
                case when wes.is_hide = 1 then '取消合作' else '合作' end as status,
                case when wes.supplier_genre = 1 then '供应商一'
                        when wes.supplier_genre = 2 then '供应商二'
                        when wes.supplier_genre = 3 then '取消合作'
                        when wes.supplier_genre = 4 then 'MV'
                        when wes.supplier_genre = 5 then '日韩'
                        when wes.supplier_genre = 6 then '沙特本地供应商'
                        when wes.supplier_genre = 9 then '土耳其本地供应商'
                        when wes.supplier_genre = 10 then '阿联酋其本地供应商'
                        when wes.supplier_genre = 11 then 'POP'
                        else null end as provider_type,
                from_unixtime(wes.great_time, "yyyyMMdd") as great_time
            from jolly.who_esoloo_supplier as wes
        ) as base
        left join zybiro.bi_elaine_supp_name as supp_name 
        on base.code = supp_name.code
        left join(
            select
                dg.provider_code,
                substr(dge.data_date,1,6) as year_month,
                count(distinct dg.goods_id) as goods_num,
                count(distinct case when from_timestamp(dg.first_on_sale_time,'yyyyMM') = substr(dge.data_date,1,6) then dg.goods_id else null end) as new_goods_num,
                count(distinct wsku.rec_id) as sku_num
            from dim.dim_goods as dg
            left join dim.dim_goods_extend as dge
            on dg.goods_id = dge.goods_id
            left join jolly.who_sku_relation as wsku
            on dg.goods_id = wsku.goods_id
            left join dim.dim_goods_category_group_new as cat
            on dg.cate_level1_name =cat.cate_level1_name
            where cat.category_group in ('beauty','家居','孕婴童用品','婴童时尚')
            and dge.data_date >= '{begin}'
            and dge.data_date <= '{end}'
            and dge.is_jc_on_sale = 1
            and dge.is_jc_sale = 1
            -- and dg.provider_code = '66F'
            group by 1,2
        ) as on_sales
        on base.code = on_sales.provider_code
        left join(
            select
                c.provider_code,
                from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMM') as year_month,
                count(distinct b.goods_id) as sale_goods_num,
                sum(b.original_goods_number) num,
                sum(b.original_goods_number*b.goods_price) revenue,
                sum(b.original_goods_number*b.in_price_usd) cost_usd,
                sum(b.original_goods_number*b.in_price) cost_rmb,
                sum(b.original_goods_number*b.in_price_usd) +nvl(sum( case when ((lower(a.country_name)='saudi arabia' and e.is_sa_supplier<>1) 
                    or  (lower(a.country_name)='united arab emirates' and e.supplier_genre<>10)) then b.goods_price*b.original_goods_number*0.05
                    when lower(a.country_name) in ('saudi arabia','united arab emirates')
                    then  (b.goods_price-b.in_price_usd)*b.original_goods_number/1.05*0.05 end),0) cost_with_vat
            from dw.dw_order_goods_fact b 
            inner join  dw.dw_order_fact a on a.order_id=b.order_id
            left join dim.dim_goods c on c.goods_id=b.goods_id
            left join jolly.who_esoloo_supplier  e on  c.provider_code = e.code
            left join dim.dim_goods_category_group_new as cat
            on c.cate_level1_name =cat.cate_level1_name
            where
            from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >='{begin}'
            and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') <='{end}'
            and a.site_id  in(400,600,700,601,900) 
            and a.pay_status in(1,3)
            and c.supplier_genre<>11
            and cat.category_group in ('beauty','家居','孕婴童用品','婴童时尚')
            group by
            1,2
        ) as sales
        on on_sales.provider_code = sales.provider_code
        and on_sales.year_month = sales.year_month
        -- where code = '66F'
    ),

    main_supp as (
        select
            rn.provider_code,
            rn.cate_level1_name
        from(
            select
                main_cate.provider_code,
                main_cate.cate_level1_name,
                revenue,
                row_number() over(partition by main_cate.provider_code order by main_cate.revenue desc) as rn
            from(
                select
                    c.cate_level1_name,
                    c.provider_code,
                    sum(b.original_goods_number*b.goods_price) revenue
                from dw.dw_order_goods_fact b 
                inner join  dw.dw_order_fact a on a.order_id=b.order_id
                left join dim.dim_goods c on c.goods_id=b.goods_id
                left join jolly.who_esoloo_supplier  e on  c.provider_code = e.code
                where
                from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >='{begin}'
                and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') <='{end}'
                and a.site_id  in(400,600,700,601,900) 
                and a.pay_status in(1,3)
                and c.supplier_genre<>11
                group by 1,2
            ) as main_cate
        ) as rn
        where rn.rn=1
        -- and provider_code = '66F'
    ),

    stocks as (
        select 
            dg.provider_code,
            sum(total_stock_num - total_order_lock_num - total_return_lock_num - total_allocate_lock_num) as free_stock_num
        from ods.ods_who_wms_goods_stock_total_detail as stock
        left join dim.dim_goods as dg on stock.goods_id = dg.goods_id
        left join dim.dim_goods_category_group_new as cat
        on dg.cate_level1_name = cat.cate_level1_name
        where cat.category_group in ('beauty','家居','孕婴童用品','婴童时尚')
        and stock.data_date = '{end}'
        group by 1
    )


    select
        base.supp_id,
        base.code,
        base.supp_name,
        base.status,
        base.provider_type,
        main_supp.cate_level1_name,
        base.great_time,
        stocks.free_stock_num,
        base.year_month,
        base.goods_num,
        base.new_goods_num,
        base.sale_goods_num,
        base.sku_num,
        base.num,
        base.revenue,
        base.cost_rmb,
        base.cost_with_vat
    from base left join main_supp
    on base.code = main_supp.provider_code
    left join stocks
    on base.code = stocks.provider_code
    where base.status != "取消合作"
    and base.year_month is not null
    """.format(begin=begin, end=end)
    return sqlmsg


def quarter_goods():
    sqlmsg = """
    with goods as (
        select
            cat.category_group,
            case when dg.cate_level1_name = "Beauty" then dg.cate_level2_name else dg.cate_level1_name end as cate_level1_name,
            case when dg.cate_level1_name = "Beauty" then dg.cate_level3_name else dg.cate_level2_name end as cate_level2_name,
            case when dg.cate_level1_name = "Beauty" then dg.cate_level4_name else dg.cate_level3_name end as cate_level3_name,
            dg.goods_id,
            qg.goods_type,
            dg.goods_sn,
            dg.goods_name,
            dg.first_on_sale_time
        from dim.dim_goods as dg
        left join dim.dim_goods_extend as dge
        on dg.goods_id = dge.goods_id
        and dge.data_date >= from_timestamp(date_sub(now(), 7),'yyyyMMdd')
        and dge.data_date <= from_timestamp(date_sub(now(), 1),'yyyyMMdd')
        left join dim.dim_goods_category_group_new as cat
        on dg.cate_level1_name = cat.cate_level1_name
        left join zybiro.bi_longjy_the_quarter_goods as qg
        on dg.goods_id = qg.goods_id
        where cat.category_group in ('beauty','家居','孕婴童用品','婴童时尚')
        and dg.supplier_genre != 11
        and dge.is_jc_on_sale = 1
        and qg.goods_id is not null
        group by 1,2,3,4,5,6,7,8,9
    ),
    
    -- 期初库存
    stock_begin as (
        select
            gstd.goods_id,
            case when gstd.depot_id = 15 then "UAE"
                when gstd.depot_id = 16 then "SA"
                else "国内仓" end as depot ,
            sum(case when gstd.data_date = from_timestamp(date_sub(now(), 7),'yyyyMMdd') then gstd.total_stock_num else 0 end) as total_stock_num_begin,
            sum(case when gstd.data_date = from_timestamp(date_sub(now(), 7),'yyyyMMdd') then gstd.free_stock_num else 0 end) as free_stock_num_begin,
            sum(case when gstd.data_date = from_timestamp(date_sub(now(), 1),'yyyyMMdd') then gstd.total_stock_num else 0 end) as total_stock_num_end,
            sum(case when gstd.data_date = from_timestamp(date_sub(now(), 1),'yyyyMMdd') then gstd.free_stock_num else 0 end) as free_stock_num_end
        from ods.ods_who_wms_goods_stock_total_detail as gstd
        left join dim.dim_goods as dg
        on gstd.goods_id = dg.goods_id
        left join dim.dim_goods_category_group_new as cat
        on dg.cate_level1_name = cat.cate_level1_name
        where cat.category_group in ('beauty','家居','孕婴童用品','婴童时尚')
        and (gstd.data_date = from_timestamp(date_sub(now(), 7),'yyyyMMdd')
            or gstd.data_date = from_timestamp(date_sub(now(), 1),'yyyyMMdd'))
        group by 1,2
    ),
    
    -- 库存变化情况
    stock_change as (
        select
            a.goods_id,
            case when a.depot_id = 15 then "UAE"
                when a.depot_id = 16 then "SA"
                else "国内仓" end as depot,
            sum(change_num) as change_num,
            -- 出库
            sum(case when a.change_type = 5 then change_num else 0 end) as sale_out,
            sum(case when a.change_type = 13 then change_num else 0 end) as to_other_depot_out,
            sum(case when a.change_type = 12 then change_num else 0 end) as return_out,
            sum(case when a.change_type in (6,10,17,19,20,21,22,24,25) then change_num else 0 end) as order_out,
            -- 入库
            sum(case when a.change_type = 1 then change_num else 0 end) as buy_in,
            sum(case when a.change_type = 3 then change_num else 0 end) as return_in,
            sum(case when a.change_type = 15 then change_num else 0 end) as other_deopt_in,
            sum(case when a.change_type in (2,4,9,11,14,15,16,18,23) then change_num else 0 end) as order_in,
            -- 昨售出库
            sum(case when a.change_type = 5 
                and from_unixtime(change_time)>= to_date(date_sub(now(), 1)) 
                then change_num else 0 end) as sale_out_yesterday,
            sum(case when a.change_type = 13 
                and from_unixtime(change_time)>= to_date(date_sub(now(), 1)) 
                then change_num else 0 end) as to_other_depot_out_yesterday,
            sum(case when a.change_type = 12 
                and from_unixtime(change_time)>= to_date(date_sub(now(), 1)) 
                then change_num else 0 end) as return_out_yesterday,
            sum(case when a.change_type in (6,10,17,19,20,21,22,24,25) 
                and from_unixtime(change_time)>= to_date(date_sub(now(), 1)) 
                then change_num else 0 end) as order_out_yesterday,
            -- 昨天入库
            sum(case when a.change_type = 1 
                and from_unixtime(change_time)>= to_date(date_sub(now(), 1)) 
                then change_num else 0 end) as buy_in_yesterday,
            sum(case when a.change_type = 3 
                and from_unixtime(change_time)>= to_date(date_sub(now(), 1)) 
                then change_num else 0 end) as return_in_yesterday,
            sum(case when a.change_type = 15 
                and from_unixtime(change_time)>= to_date(date_sub(now(), 1)) 
                then change_num else 0 end) as other_deopt_in_yesterday,
            sum(case when a.change_type in (2,4,9,11,14,15,16,18,23) 
                and from_unixtime(change_time)>= to_date(date_sub(now(), 1)) 
                then change_num else 0 end) as order_in_yesterday
        from jolly_wms.who_wms_goods_stock_detail_log a
        inner join dim.dim_goods g on a.goods_id=g.goods_id
        left join dim.dim_goods_category_group_new as cat
        on g.cate_level1_name = cat.cate_level1_name
        where cat.category_group in ('beauty' , '家居', '孕婴童用品', '婴童时尚')
        and from_unixtime(change_time) >= to_date(date_sub(now(), 7))
        and from_unixtime(change_time) < to_date(now())
        group by 1,2
    )
    
    
    select
        goods.goods_type                     as `类型`,
        goods.category_group                 as `类目`,
        goods.cate_level1_name               as `一级类目`,
        goods.cate_level2_name               as `二级类目`,
        goods.cate_level3_name               as `三级类目`,
        goods.goods_id,
        goods.goods_sn,
        goods.goods_name                     as `商品名称`,
        goods.first_on_sale_time             as `首次上架时间`,
        stock_begin.depot                    as `仓库`,
        stock_begin.total_stock_num_begin    as `期初库存数量`,
        stock_begin.free_stock_num_begin     as `期初自由库存数量`,
        stock_change.buy_in                  as `近7天采购入库`,
        stock_change.return_in               as `近7天销售退货入库`,
        stock_change.other_deopt_in          as `近7天调拨入库`,
        stock_change.order_in                as `近7天其他入库`,
        stock_change.sale_out                as `近7天销售出库`,
        stock_change.to_other_depot_out      as `近7天调拨出库`,
        stock_change.return_out              as `近7天库存退货出库`,
        stock_change.order_out               as `近7天其他出库`,
        stock_change.buy_in_yesterday        as `昨天采购入库`,
        stock_change.return_in_yesterday     as `昨天销售退货入库`,
        stock_change.other_deopt_in_yesterday as `昨天调拨入库`,
        stock_change.order_in_yesterday      as `昨天其他入库`,
        stock_change.sale_out_yesterday      as `昨天销售出库`,
        stock_change.to_other_depot_out_yesterday as `昨天调拨出库`,
        stock_change.return_out_yesterday    as `昨天库存退货出库`,
        stock_change.order_out_yesterday     as `昨天其他出库`,
        stock_begin.total_stock_num_end      as `期末库存数量`,
        stock_begin.free_stock_num_end       as `期末自由库存数量`
    from goods left join stock_begin
    on goods.goods_id = stock_begin.goods_id
    left join stock_change
    on stock_begin.goods_id = stock_change.goods_id
    and stock_begin.depot = stock_change.depot
    where stock_begin.depot is not null
    """
    return sqlmsg


def depot_stock_goods():
    sqlmsg = """
    with stock_info as (
        select
            cat.category_group as category_group,
            case when dg.cate_level1_name = "Beauty" then dg.cate_level2_name else dg.cate_level1_name end as cate_level1_name,
            case when dg.cate_level1_name = "Beauty" then dg.cate_level3_name else dg.cate_level2_name end as cate_level2_name,
            case when dg.cate_level1_name = "Beauty" then dg.cate_level4_name else dg.cate_level3_name end as cate_level3_name,
            gstd.goods_id,
            dg.goods_name as goods_name,
            dge.is_jc_on_sale as is_jc_on_sale,
            sum(gstd.total_stock_num) as total_stock_num,
            sum(case when gstd.depot_id not in (15,16) then total_stock_num else 0 end) as cn_total_stock_num,
            sum(case when gstd.depot_id = 16 then total_stock_num else 0 end) as sa_total_stock_num,
            sum(case when gstd.depot_id = 15 then total_stock_num else 0 end) as uae_total_stock_num,
            sum(gstd.free_stock_num) as free_stock_num,
            sum(case when gstd.depot_id not in (15,16) then free_stock_num else 0 end) as cn_free_stock_num,
            sum(case when gstd.depot_id = 16 then free_stock_num else 0 end) as sa_free_stock_num,
            sum(case when gstd.depot_id = 15 then free_stock_num else 0 end) as uae_free_stock_num
        from ods.ods_who_wms_goods_stock_total_detail as gstd
        left join dim.dim_goods as dg on gstd.goods_id = dg.goods_id
        left join dim.dim_goods_extend as dge 
        on dg.goods_id = dge.goods_id
        and gstd.data_date = dge.data_date
        left join dim.dim_goods_category_group_new as cat
        on dg.cate_level1_name = cat.cate_level1_name
        where cat.category_group in ('beauty', '家居', '孕婴童用品', '婴童时尚')
        and gstd.data_date = from_timestamp(date_sub(now(),1), "yyyyMMdd")
        and (gstd.total_stock_num > 0 or dge.is_jc_on_sale = 1)
        and dg.supplier_genre != 11
        group by 1,2,3,4,5,6,7
    ),

    sale_info as (
        select
            a0.goods_id,
            sum(case when data_date = from_timestamp(date_sub(now(),1), "yyyyMMdd") then a0.num else 0 end) as yestoday_num,
            sum(case when data_date >= from_timestamp(date_sub(now(),7), "yyyyMMdd") then a0.num else 0 end) as near_7_num,
            sum(case when data_date >= from_timestamp(date_sub(now(),30), "yyyyMMdd") then a0.num else 0 end) as near_30_num,
            sum(case when data_date = from_timestamp(date_sub(now(),1), "yyyyMMdd") then a0.revenue else 0 end) as yestoday_gmv,
            sum(case when data_date >= from_timestamp(date_sub(now(),7), "yyyyMMdd") then a0.revenue else 0 end) as near_7_gmv,
            sum(case when data_date >= from_timestamp(date_sub(now(),30), "yyyyMMdd") then a0.revenue else 0 end) as near_30_gmv
        from (
            select
                from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') data_date,
                c.goods_id,
                sum(b.original_goods_number) num,
                sum(b.original_goods_number*b.goods_price) revenue
            from dw.dw_order_goods_fact b 
            inner join  dw.dw_order_fact a on a.order_id=b.order_id
            left join dim.dim_goods c on c.goods_id=b.goods_id
            left join jolly.who_esoloo_supplier  e on  c.provider_code = e.code
            left join dim.dim_goods_category_group_new as cat
            on c.cate_level1_name = cat.cate_level1_name
            where
            from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >=from_timestamp(date_sub(now(),30), "yyyyMMdd")
            and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') <= from_timestamp(date_sub(now(),1), "yyyyMMdd")
            and cat.category_group in ('beauty', '家居', '孕婴童用品', '婴童时尚')
            and c.supplier_genre != 11
            and a.site_id  in(400,600,700,601,900) 
            and a.pay_status in(1,3)
            group by 1,2
        ) as a0
        group by 1
    )


    select
        ti.category_group as `类目`,
        ti.cate_level1_name as `一级`,
        ti.cate_level2_name as `二级`,
        ti.cate_level3_name as `三级`,
        ti.goods_id as `goods_id`,
        ti.goods_name as `商品名称`,
        ti.is_jc_on_sale as `是否在架`,
        ti.total_stock_num as `总库存`,
        ti.cn_total_stock_num as `国内仓库存`,
        ti.sa_total_stock_num as `SA库存`,
        ti.uae_total_stock_num as `UAE库存`,
        ti.free_stock_num as `自由库存`,
        ti.cn_free_stock_num as `国内自由库存`,
        ti.sa_free_stock_num as `SA自由库存`,
        ti.uae_free_stock_num as `UAE自由库存`,
        si.yestoday_num as `昨天销量`,
        si.near_7_num as `近7天销量`,
        si.near_30_num as `近30天销量`,
        si.yestoday_gmv as `昨天GMV`,
        si.near_7_gmv as `近7天GMV`,
        si.near_30_gmv as `近30天GMV`
    from stock_info as ti
    left join sale_info as si
    on ti.goods_id = si.goods_id
    """
    return sqlmsg


def tmp_sql():
    sql_msg = """
    with sales as (
        select
            cat.category_group,
            case when c.cate_level1_name = "Beauty" then c.cate_level2_name else c.cate_level1_name end as cate_level1_name,
            case when c.cate_level1_name = "Beauty" then c.cate_level3_name else c.cate_level2_name end as cate_level2_name,
            case when c.cate_level1_name = "Beauty" then c.cate_level4_name else c.cate_level3_name end as cate_level3_name,
            from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMM') data_date,
            case when c.supplier_genre=11 then 1 else 0 end as  is_pop,
            count(distinct a.order_id) as order_num,
            count(distinct a.user_id) as user_num,
            count(distinct b.goods_id) as sale_goods_num,
            sum(b.original_goods_number) num,
            sum(b.original_goods_number*b.goods_price) revenue,
            sum(b.original_goods_number*b.in_price_usd) +nvl(sum( case when ((lower(a.country_name)='saudi arabia' and e.is_sa_supplier<>1) 
                or  (lower(a.country_name)='united arab emirates' and e.supplier_genre<>10)) then b.goods_price*b.original_goods_number*0.05
                when lower(a.country_name) in ('saudi arabia','united arab emirates')
                then  (b.goods_price-b.in_price_usd)*b.original_goods_number/1.05*0.05 end),0) cost_with_vat
        from dw.dw_order_goods_fact b 
        inner join  dw.dw_order_fact a on a.order_id=b.order_id
        left join dim.dim_goods c on c.goods_id=b.goods_id
        left join jolly.who_esoloo_supplier  e on  c.provider_code = e.code
        left join dim.dim_goods_category_group_new as cat
        on c.cate_level1_name = cat.cate_level1_name
        where
        from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >= '20180101'
        and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') <= '20191231'
        and a.site_id  in(400,600,700,601,900) 
        and a.pay_status in(1,3)
        and cat.category_group in ('beauty', '家居','婴童时尚','孕婴童用品')
        group by 1,2,3,4,5,6
    ),
    
    goods as (
        select
            cat.category_group,
            case when dg.cate_level1_name = "Beauty" then dg.cate_level2_name else dg.cate_level1_name end as cate_level1_name,
            case when dg.cate_level1_name = "Beauty" then dg.cate_level3_name else dg.cate_level2_name end as cate_level2_name,
            case when dg.cate_level1_name = "Beauty" then dg.cate_level4_name else dg.cate_level3_name end as cate_level3_name,
            substr(dge.data_date,1,6) as year_month,
            case when dg.supplier_genre=11 then 1 else 0 end as is_pop,
            count(distinct dg.goods_id) as goods_num
        from dim.dim_goods as dg
        left join dim.dim_goods_extend as dge
        on dg.goods_id = dge.goods_id
        and dge.data_date >= '20180101'
        and dge.data_date <= '20191231'
        left join dim.dim_goods_category_group_new as cat
        on dg.cate_level1_name = cat.cate_level1_name
        where dge.is_jc_on_sale = 1
        and cat.category_group in ('beauty', '家居','婴童时尚','孕婴童用品')
        group by 1,2,3,4,5,6
    ),
    
    dau as (
        select
            cat.category_group,
            case when dg.cate_level1_name = "Beauty" then dg.cate_level2_name else dg.cate_level1_name end as cate_level1_name,
            case when dg.cate_level1_name = "Beauty" then dg.cate_level3_name else dg.cate_level2_name end as cate_level2_name,
            case when dg.cate_level1_name = "Beauty" then dg.cate_level4_name else dg.cate_level3_name end as cate_level3_name,
            case when dg.supplier_genre = 11 then 1 else 0 end as is_pop,
            substr(rsgd.data_date,1,6) as year_month,
            sum(rsgd.`expo_pv`) as expo_pv,
            sum(rsgd.goods_click_uv) as goods_click_uv,
            sum(rsgd.sales_uv) as sales_uv,
            sum(rsgd.sales_uv)/sum(rsgd.goods_click_uv) as rate
        from rpt.rpt_sum_goods_daily as rsgd
        left join dim.dim_goods as dg
        on rsgd.goods_id = dg.goods_id
        left join dim.dim_goods_category_group_new as cat
        on dg.cate_level1_name = cat.cate_level1_name
        where cat.category_group in ('beauty', '家居','婴童时尚','孕婴童用品')
        group by 1,2,3,4,5,6
    )
    
    
    select
        goods.category_group,
        goods.cate_level1_name,
        goods.cate_level2_name,
        goods.cate_level3_name,
        goods.year_month as `年月`,
        goods.is_pop as `是否pop`,
        goods.goods_num as `在架款数`,
        sales.order_num as `订单数`,
        sales.user_num as `用户数`,
        sales.sale_goods_num as `销售款数`,
        sales.num as `销量`,
        sales.revenue as `GMV`,
        sales.cost_with_vat as `成本`,
        dau.expo_pv as `曝光pv`,
        dau.goods_click_uv as `点击uv`,
        dau.sales_uv as `销售uv`,
        sales.num/goods.goods_num as `在架商品均销量`,
        sale_goods_num/goods.goods_num as `动销率`,
        sales.revenue/sales.num as `件均价`,
        dau.rate as `购买转化率`,
        1- sales.cost_with_vat/sales.revenue as `毛利率`
    from goods left join sales
    on goods.category_group = sales.category_group
    and goods.cate_level1_name = sales.cate_level1_name
    and goods.cate_level2_name = sales.cate_level2_name
    and goods.cate_level3_name = sales.cate_level3_name
    and goods.is_pop = sales.is_pop
    and goods.year_month = sales.data_date
    left join dau
    on goods.category_group = dau.category_group
    and goods.cate_level1_name = dau.cate_level1_name
    and goods.cate_level2_name = dau.cate_level2_name
    and goods.cate_level3_name = dau.cate_level3_name
    and goods.is_pop = dau.is_pop
    and sales.data_date=dau.year_month
    """
    return sql_msg


def price_brand():
    sqlmsg = """
    select
        cat.category_group,
        case when dg.cate_level1_name='Beauty' then dg.cate_level2_name else dg.cate_level1_name end as cate_level1_name,
        case when dg.cate_level1_name='Beauty' then dg.cate_level3_name else dg.cate_level2_name end as cate_level2_name,
        case when dg.cate_level1_name='Beauty' then dg.cate_level4_name else dg.cate_level3_name end as cate_level3_name,
        count(case when dge.in_price > 0 and dge.in_price <= 2 then dg.goods_id end) as "(0, 2]",
        count(case when dge.prst_price_1 > 2 and dge.prst_price_1 <= 4 then dg.goods_id end) as "(2, 4]",
        count(case when dge.prst_price_1 > 4 and dge.prst_price_1 <= 6 then dg.goods_id end) as "(4, 6]",
        count(case when dge.prst_price_1 > 6 and dge.prst_price_1 <= 8 then dg.goods_id end) as "(6, 8]",
        count(case when dge.prst_price_1 > 8 and dge.prst_price_1 <= 10 then dg.goods_id end) as "(8, 10]",
        count(case when dge.prst_price_1 > 10 and dge.prst_price_1 <= 12 then dg.goods_id end) as "(10, 12]",
        count(case when dge.prst_price_1 > 12 and dge.prst_price_1 <= 14 then dg.goods_id end) as "(12, 14]",
        count(case when dge.prst_price_1 > 14 and dge.prst_price_1 <= 16 then dg.goods_id end) as "(14, 16]",
        count(case when dge.prst_price_1 > 16 and dge.prst_price_1 <= 18 then dg.goods_id end) as "(16, 18]",
        count(case when dge.prst_price_1 > 18 and dge.prst_price_1 <= 20 then dg.goods_id end) as "(18, 20]",
        count(case when dge.prst_price_1 > 20 and dge.prst_price_1 <= 30 then dg.goods_id end) as "(20, 30]",
        count(case when dge.prst_price_1 > 30 and dge.prst_price_1 <= 40 then dg.goods_id end) as "(30, 40]",
        count(case when dge.prst_price_1 > 40 and dge.prst_price_1 <= 50 then dg.goods_id end) as "(40, 50]",
        count(case when dge.prst_price_1 > 50 and dge.prst_price_1 <= 60 then dg.goods_id end) as "(50, 60]",
        count(case when dge.prst_price_1 > 60 and dge.prst_price_1 <= 80 then dg.goods_id end) as "(60, 80]",
        count(case when dge.prst_price_1 > 80 and dge.prst_price_1 <= 100 then dg.goods_id end) as "(80, 100]",
        count(case when dge.prst_price_1 > 100 and dge.prst_price_1 <= 200 then dg.goods_id end) as "(100, 200]",
        count(case when dge.prst_price_1 > 200 and dge.prst_price_1 <= 300 then dg.goods_id end) as "(200, 300]",
        count(case when dge.prst_price_1 > 300 and dge.prst_price_1 <= 400 then dg.goods_id end) as "(300, 400]",
        count(case when dge.prst_price_1 > 400 and dge.prst_price_1 <= 500 then dg.goods_id end) as "(400, 500]",
        count(case when dge.prst_price_1 > 500 and dge.prst_price_1 <= 800 then dg.goods_id end) as "(500, 800]",
        count(case when dge.prst_price_1 > 800 and dge.prst_price_1 <= 1000 then dg.goods_id end) as "(800, 1000]",
        count(case when dge.prst_price_1 > 1000 and dge.prst_price_1 <= 9999 then dg.goods_id end) as "(1000, 9999]"
    from dim.dim_goods as dg
    left join dim.dim_goods_category_group_new as cat
    on dg.cate_level1_name = cat.cate_level1_name
    left join dim.dim_goods_extend as dge
    on dg.goods_id = dge.goods_id
    where cat.category_group  in ('家居', 'beauty', '孕婴童用品', '婴童时尚')
    and dg.supplier_genre != 11
    and dge.data_date = from_timestamp(date_sub(now(),1), "yyyyMMdd")
    and dge.is_jc_on_sale = 1
    group by 1,2,3,4
    order by cat.category_group,
        case when dg.cate_level1_name='Beauty' then dg.cate_level2_name else dg.cate_level1_name end,
        case when dg.cate_level1_name='Beauty' then dg.cate_level3_name else dg.cate_level2_name end,
        case when dg.cate_level1_name='Beauty' then dg.cate_level4_name else dg.cate_level3_name end;
    """
    return sqlmsg


def volume_weight():
    sqlmsg = """
    select
        cat.category_group,
        dg.cate_level1_name,
        dg.goods_id,
        wsr.rec_id,
        wsr.sku_length,
        wsr.sku_width,
        wsr.sku_height,
        wsr.sku_weight,
        (wsr.sku_length*wsr.sku_width*wsr.sku_height)/5000 as `体积重量`,
        ((wsr.sku_length*wsr.sku_width*wsr.sku_height)/5000)/wsr.sku_weight as `抛重比`
    from jolly.who_sku_relation as wsr
    left join dim.dim_goods as dg
    on wsr.goods_id = dg.goods_id
    left join dim.dim_goods_extend as dge
    on dg.goods_id = dge.goods_id
    and dge.data_date = '20191118'
    left join dim.dim_goods_category_group_new as cat
    on dg.cate_level1_name = cat.cate_level1_name
    where dg.supplier_genre<>11
    and cat.category_group in ('家居','beauty','婴童时尚','孕婴童用品')
    and ((wsr.sku_length*wsr.sku_width*wsr.sku_height)/5000)/wsr.sku_weight > 1
    and dge.is_jc_on_sale=1
    """
    return sqlmsg


def day_stock_msg_english():
    sqlmsg = """
    select
        sga.category_group_1 as `类目`,
        case when sga.category_group_1 = 'beauty' then cat_level2_name else cat_level1_name end as `cat_level1_name`,
        case when sga.category_group_1 = 'beauty' then cat_level3_name else cat_level2_name end as `cat_level2_name`,
        case when sga.category_group_1 = 'beauty' then cat_level4_name else cat_level3_name end as `cat_level3_name`,
        sga.goods_id,
        sga.goods_name,
        sga.goods_season,
        sga.first_on_sale_time,
        sga.in_price as `in_price(￥)`,
        sga.in_price_usd as `in_price($)`,
        sga.shop_price_1 as `shop_price`,
        sga.prst_price_1 as `prst_price`,
        sga.stock as `stock`,
        sga.stock_sa as `stock_sa`,
        sga.stock_uae as `stock_uae`,
        sga.free_stock as `free_stock`,
        sga.free_stock_sa as `free_stock_sa`,
        sga.free_stock_uae as `free_stock_uae`,
        sga.onway as `onway`,
        sga.age_zx_sa as `age_zx_sa`,
        sga.age_zx_uae as `age_zx_uae`,
        sga.goods_num_30 as `goods_num_30`,
        sga.goods_revenue_30 as `goods_revenue_30`,
        sga.goods_revenue_30_m as `goods_margin_30_m`,
        sga.goods_num_30/sga.free_stock as `Days available`,
        sga.goods_reason_return_180d as `goods_reason_return_180d`,
        sga.goods_reason_return_rate_180d as `goods_reason_return_rate_180d`,
        sga.goods_reason_return_30d as `goods_reason_return_30d`,
        sga.goods_reason_return_rate_30d as `goods_reason_return_rate_30d`,
        sga.bad_comments_rate as `bad_comments_rate`
    from zybiro.bi_chili_syb_goods_all as sga
    left join dim.dim_goods as dg
    on sga.goods_id = dg.goods_id
    where ds = from_timestamp(date_sub(now(),1), "yyyyMMdd")
    and category_group_1 in ('beauty','家居','婴童时尚','孕婴童用品')
    and sga.stock_sa > 0
    and dg.supplier_genre != 11
    order by sga.category_group_1, 
        case when sga.category_group_1 = 'beauty' then cat_level2_name else cat_level1_name end,
        case when sga.category_group_1 = 'beauty' then cat_level3_name else cat_level2_name end,
        case when sga.category_group_1 = 'beauty' then cat_level4_name else cat_level3_name end
    """
    return sqlmsg


def goods_view():
    sqlmsg = """
    select
        distinct regexp_replace(to_date(from_unixtime(a.gmt_create)),'-','') `视频创建日期`,
        case when dg.cate_level1_name = "Beauty" then dg.cate_level2_name else dg.cate_level1_name end as `一级`,
        case when dg.cate_level1_name = "Beauty" then dg.cate_level3_name else dg.cate_level1_name end as `二级`,
        case when dg.cate_level1_name = "Beauty" then dg.cate_level4_name else dg.cate_level1_name end as `三级`,
        a.goods_id,
        dg.goods_sn,
        dg.goods_name as `商品名称`,
        case when supplier_genre = 11 then 'POP' else 'Self' end as `是否POP`
    from jolly.who_goods_video a
    join dim.dim_goods dg on a.goods_id = dg.goods_id
    left join dim.dim_goods_category_group_new as cat
    on dg.cate_level1_name = cat.cate_level1_name
    where cat.category_group in ('beauty','家居','孕婴童用品','婴童时尚')
    and a.video_status=1
    and is_delete = 0
    """
    return sqlmsg


def goods_stock_data():
    sqlmsg = """
    select
        sga.category_group_1 as `类目`,
        case when sga.category_group_1 = 'beauty' then cat_level2_name else cat_level1_name end as `一级`,
        case when sga.category_group_1 = 'beauty' then cat_level3_name else cat_level2_name end as `二级`,
        case when sga.category_group_1 = 'beauty' then cat_level4_name else cat_level3_name end as `三级`,
        sga.goods_id as `id`,
        sga.goods_name as `商品名称`,
        sga.goods_season as `季节`,
        sga.first_on_sale_time as `上架时间`,
        sga.in_price as `成本(￥)`,
        sga.in_price_usd as `成本($)`,
        sga.shop_price_1 as `原价`,
        sga.prst_price_1 as `促销价`,
        sga.stock as `总库存`,
        sga.stock_sa as `sa总库存`,
        sga.stock_uae as `uae总库存`,
        sga.free_stock as `自由库存`,
        sga.free_stock_sa as `sa自由库存`,
        sga.free_stock_uae as `uae自由库存`,
        sga.onway as `在途库存`,
        sga.age_zx_sa as `滞销等级sa`,
        sga.age_zx_uae as `滞销等级uae`,
        sga.goods_num_30 as `近30天销量`,
        sga.goods_revenue_30 as `近30天GMV`,
        sga.goods_revenue_30_m as `近30天毛利率`,
        sga.goods_num_30/sga.free_stock as `可销天数`,
        sga.goods_reason_return_180d as `退货件数-180天`,
        sga.goods_reason_return_rate_180d as `退货率-180天`,
        sga.goods_reason_return_30d as `退货件数-30天`,
        sga.goods_reason_return_rate_30d as `退货率-30天`,
        sga.bad_comments_rate as `差评率-180天`
    from zybiro.bi_chili_syb_goods_all as sga
    left join dim.dim_goods as dg
    on sga.goods_id = dg.goods_id
    where ds = from_timestamp(date_sub(now(),1), "yyyyMMdd")
    and category_group_1 in ('beauty','家居','婴童时尚','孕婴童用品')
    and dg.supplier_genre != 11
    order by sga.category_group_1,
        case when sga.category_group_1 = 'beauty' then cat_level2_name else cat_level1_name end,
        case when sga.category_group_1 = 'beauty' then cat_level3_name else cat_level2_name end,
        case when sga.category_group_1 = 'beauty' then cat_level4_name else cat_level3_name end
    """
    return sqlmsg


def sku_stock_data():
    sqlmsg = """
        select
            cat.category_group as category_group,
            case when dg.cate_level1_name = "Beauty" then dg.cate_level2_name else dg.cate_level1_name end as cate_level1_name,
            case when dg.cate_level1_name = "Beauty" then dg.cate_level3_name else dg.cate_level2_name end as cate_level2_name,
            case when dg.cate_level1_name = "Beauty" then dg.cate_level4_name else dg.cate_level3_name end as cate_level3_name,
            gstd.goods_id,
            dg.goods_name as goods_name,
            gstd.sku_id,
            dge.is_jc_on_sale as is_jc_on_sale,
            sum(gstd.total_stock_num) as total_stock_num,
            sum(case when gstd.depot_id not in (15, 16, 36) then total_stock_num else 0 end) as cn_total_stock_num,
            sum(case when gstd.depot_id in (16, 36) then total_stock_num else 0 end) as sa_total_stock_num,
            sum(case when gstd.depot_id = 15 then total_stock_num else 0 end) as uae_total_stock_num,
            sum(gstd.free_stock_num) as free_stock_num,
            sum(case when gstd.depot_id not in (15, 16, 36) then free_stock_num else 0 end) as cn_free_stock_num,
            sum(case when gstd.depot_id in (16, 36) then free_stock_num else 0 end) as sa_free_stock_num,
            sum(case when gstd.depot_id = 15 then free_stock_num else 0 end) as uae_free_stock_num
        from ods.ods_who_wms_goods_stock_total_detail as gstd
        left join dim.dim_goods as dg on gstd.goods_id = dg.goods_id
        left join dim.dim_goods_extend as dge 
        on dg.goods_id = dge.goods_id
        and gstd.data_date = dge.data_date
        left join dim.dim_goods_category_group_new as cat
        on dg.cate_level1_name = cat.cate_level1_name
        where cat.category_group in ('beauty', '家居', '孕婴童用品', '婴童时尚')
        and gstd.data_date = from_timestamp(date_sub(now(),1), "yyyyMMdd")
        and (gstd.total_stock_num > 0 or dge.is_jc_on_sale = 1)
        group by 1,2,3,4,5,6,7,8
        order by cat.category_group,
            case when dg.cate_level1_name = "Beauty" then dg.cate_level2_name else dg.cate_level1_name end,
            case when dg.cate_level1_name = "Beauty" then dg.cate_level3_name else dg.cate_level2_name end,
            case when dg.cate_level1_name = "Beauty" then dg.cate_level4_name else dg.cate_level3_name end
    """
    return sqlmsg


def goods_tmp_lyy():
    sqlmsg = """
    with basce as 
    (
        select
            from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyy-MM-dd') data_date,
            cat.category_group,
            case when c.cate_level1_name="Beauty" then c.cate_level2_name else c.cate_level1_name end as cate_level1_name
            ,b.order_id 
            ,a.user_id
            ,b.goods_id
            ,sum(b.original_goods_number) num
            --,sum(b.original_goods_number*b.goods_price) revenue
        from	
        dw.dw_order_goods_fact b inner join  dw.dw_order_fact a on a.order_id=b.order_id
        left join dim.dim_goods c on c.goods_id=b.goods_id
        left join dim.dim_goods_category_group_new as cat
        on c.cate_level1_name = cat.cate_level1_name
        where
        from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >='20191001'
        --and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') <'${end_day}'
        and a.site_id  in(400,600,700,601,900) 
        and a.pay_status in(1,3)
        and cat.category_group in ('家居','beauty', '大件家居', '孕婴童用品', '婴童时尚')
        and c.supplier_genre<>11 
        group by 
        1,2,3,4,5,6
    ),
    income as (
        select 
            a.goods_id,
            sum(gsn) gsn, -- 发运量
            sum(a.gsngp) gsnp, -- 发运金额
            sum(a.gsnip) gsnip, -- 发运成本
            sum(a.income) income,-- 签收金额
            sum(a.discountamount) discountamount, -- 返点
            sum(a.cost) cost, -- 签收成本
            sum(a.newshippingfees) newshippingfees,-- 运费
            sum(a.thedepotfees) thedepotfees, -- 仓库费用
            sum(vat) vat, --增值税 
            sum(a.duty) duty, -- 关税
            sum(a.income)+sum(a.discountamount)-sum(a.cost)-sum(a.newshippingfees)-sum(a.thedepotfees)-sum(vat)-sum(a.duty) j_income-- 净利额
        from zybiro.bi_damon_netprofit_2018 a  -- 统一采用damon备份表，下午才能更新
        inner join dw.dw_order_sub_order_fact b
        on a.order_id=b.order_id
        inner join dim.dim_goods p2
        on a.goods_id=p2.goods_id
        left join dim.dim_goods_category_group_new as cat
        on p2.cate_level1_name = cat.cate_level1_name
        where regexp_replace(substr(case when b.pay_id=41 then b.pay_time else b.result_pay_time end,1,10 ),'-','')>='20191001'
        and regexp_replace(substr(case when b.pay_id=41 then b.pay_time else b.result_pay_time end,1,10 ),'-','')<from_timestamp(date_sub(now(),1),'yyyyMMdd')
        and b.site_id in (400,700,600,900,601)
        and b.pay_status in(1,3)
        and p2.supplier_genre<>11  -- 剔除pop供应商
        and cat.category_group in ('家居','beauty', '大件家居', '孕婴童用品', '婴童时尚')
        group by 1
    ),

    base1 as 
    (
        select
            category_group
            ,cate_level1_name
            ,goods_id 
            ,data_date
            ,order_id
            ,user_id
            ,num
            ,row_number()  over (partition by goods_id,user_id order by data_date) user_goods_bytime
        from basce
    )

    select
        a0.category_group,
        a0.cate_level1_name,
        a0.goods_id,
        a0.num,
        a1.num_fenzi,
        ic.gsn as `发运量`,
        ic.gsnp as `发运金额`,
        ic.gsnip as `发运成本`,
        ic.income as `签收金额`,
        ic.discountamount as `返点`,
        ic.cost as `签收成本`,
        ic.newshippingfees as `运费`,
        ic.thedepotfees as `仓库费用`,
        ic.vat as `增值税`,
        ic.duty as `关税`,
        ic.j_income as `净利额`
    from(
        select
            basce.category_group,
            basce.cate_level1_name,
            basce.goods_id,
            sum(basce.num) as num
        from basce
        group by 1,2,3
    ) as a0
    left join(
        select
            aa.goods_id,
            sum(aa.num_fenzi) as num_fenzi
        from(
            select  --第一种一次买两个
                goods_id,
                num  as num_fenzi
            from basce
            where 
            num >= 2 
            union  --第二张间隔一个月内买两个
            select 
                ws.goods_id,
                ws.num as num_fenzi
            from base1 ws
            left join base1 ws1 on  ws.goods_id = ws1.goods_id and ws.user_id = ws1.user_id 
            and (ws.user_goods_bytime = ws1.user_goods_bytime -1) 
            where datediff(ws1.data_date,ws.data_date)<=30
            order by ws.goods_id,ws.user_id,ws.data_date
        ) as aa
        group by 1
    ) as a1
    on a0.goods_id = a1.goods_id
    left join income as ic
    on a0.goods_id = ic.goods_id
    """
    return sqlmsg

def fg_group_sql(begin, end):
    sqlmsg = """
    select
        orders.order_id,
        goods.goods_id
    from dw.dw_order_fact as orders
    left join dw.dw_order_goods_fact as goods
    on orders.order_id = goods.order_id
    left join dim.dim_goods c
    on c.goods_id=goods.goods_id
    left join dim.dim_goods_category_group_new as cat
    on c.cate_level1_name = cat.cate_level1_name
    where from_timestamp(case when orders.pay_id=41 then orders.pay_time else orders.result_pay_time end,'yyyyMMdd') >='{begin_date}'
    and from_timestamp(case when orders.pay_id=41 then orders.pay_time else orders.result_pay_time end,'yyyyMMdd') <='{end_date}'
    and orders.site_id  in(400,600,700,601,900) 
    and orders.pay_status in(1,3)
    and c.supplier_genre != 11
    and cat.category_group in('beauty','家居','婴童时尚','孕婴童用品');
    """.format(begin_date=begin, end_date=end)
    return sqlmsg


def sku_tmp(rn_begin, rn_end):
    sqlmsg = """
    select
        *
    from(
        select
            cat.category_group,
            dg.cate_level1_id,
            dg.cate_level1_name,
            dg.cate_level2_id,
            dg.cate_level2_name,
            dg.cate_level3_id,
            dg.cate_level3_name,
            dg.cate_level4_id,
            dg.cate_level4_name,
            dg.cate_level5_id,
            dg.cate_level5_name,
            dg.goods_id,
            skur.rec_id,
            skur.in_price,
            skur.in_price_usd,
            row_number() over(order by skur.rec_id) as rn
        from ods.ods_who_sku_relation as skur
        left join dim.dim_goods as dg
        on skur.goods_id = dg.goods_id
        left join dim.dim_goods_extend as dge
        on dg.goods_id = dge.goods_id
        left join dim.dim_goods_category_group_new as cat
        on dg.cate_level1_name = cat.cate_level1_name
        where cat.category_group = '婴童时尚'
        and dge.data_date = from_timestamp(date_sub(now(),1),'yyyyMMdd')
        and dge.data_date = skur.data_date
        and dg.supplier_genre not in (6, 9, 10, 11)
    ) as a0
    where rn >= {begin}
    and rn < {end}
    """.format(begin=rn_begin, end=rn_end)
    return sqlmsg


def provider_2019_month_tmp():
    sqlmsg = """
    select
        from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMM') data_date,
        cat.category_group,
        case when c.cate_level1_name="Beauty" then c.cate_level2_name else c.cate_level1_name end as cate_level1_name,
        case when c.cate_level1_name="Beauty" then c.cate_level3_name else c.cate_level2_name end as cate_level2_name,
        case when c.cate_level1_name="Beauty" then c.cate_level4_name else c.cate_level3_name end as cate_level3_name,
        c.provider_code,
        sum(b.original_goods_number) num,
        sum(b.original_goods_number*b.goods_price) revenue,
        sum(b.original_goods_number*b.in_price_usd) +nvl(sum( case when ((lower(a.country_name)='saudi arabia' and e.is_sa_supplier<>1) 
            or  (lower(a.country_name)='united arab emirates' and e.supplier_genre<>10)) then b.goods_price*b.original_goods_number*0.05
            when lower(a.country_name) in ('saudi arabia','united arab emirates')
            then  (b.goods_price-b.in_price_usd)*b.original_goods_number/1.05*0.05 end),0) cost_with_vat
    from dw.dw_order_goods_fact b 
    inner join  dw.dw_order_fact a on a.order_id=b.order_id
    left join dim.dim_goods c on c.goods_id=b.goods_id
    left join jolly.who_esoloo_supplier  e on  c.provider_code = e.code
    left join dim.dim_goods_category_group_new as cat
    on c.cate_level1_name = cat.cate_level1_name
    where
    from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >= '20190101'
    and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') <= '20191231'
    and cat.category_group in ('家居','beauty', '大件家居', '孕婴童用品', '婴童时尚')
    and a.site_id  in(400,600,700,601,900) 
    and a.pay_status in(1,3)
    and c.supplier_genre != 11
    group by 1,2,3,4,5,6
    """
    return sqlmsg

def onsale_sql():
    sqlmsg = """
    select 
        t2.category_group
        ,t.* 
    from zybiro.month_goods_on_sale_info t  
    inner join dim.dim_goods t1 on t.goods_id=t1.goods_id
    inner join (select * from dim.dim_goods_category_group where category_group in ('婚庆周边','家居','母婴','童玩','婴童时尚','孕婴童用品')) t2 
    on t1.cate_level1_name=t2.cate_level1_name
    and t1.cate_level2_name=t2.cate_level2_name
    """


def stock_inprice():
    sqlmsg = """
    with stock_info as (
        select
            case when cat.category_group in ('家居', '孕婴童用品', '婴童时尚') then '家居事业部'
                when cat.category_group = 'beauty' then '美妆事业部'
                else null end as category_group,
            dg.provider_code,
            sup.supp_name,
            gstd.goods_id,
            sum(gstd.total_stock_num) as total_stock_num,
            sum(gstd.total_stock_num * dge.in_price_usd) as total_stock_price
        from ods.ods_who_wms_goods_stock_total_detail as gstd
        left join dim.dim_goods as dg on gstd.goods_id = dg.goods_id
        left join dim.dim_goods_extend as dge 
        on dg.goods_id = dge.goods_id
        and gstd.data_date = dge.data_date
        left join zybiro.bi_elaine_supp_name as sup
        on dg.provider_code = sup.code
        left join dim.dim_goods_category_group_new as cat
        on dg.cate_level1_name = cat.cate_level1_name
        where cat.category_group in ('beauty', '家居', '孕婴童用品', '婴童时尚')
        and gstd.data_date = from_timestamp(date_sub(now(),1), "yyyyMMdd")
        and dg.supplier_genre != 11
        group by 1,2,3,4
    ),

    sales_info as (
        select
            case when cat.category_group in ('家居', '孕婴童用品', '婴童时尚') then '家居事业部'
                when cat.category_group = 'beauty' then '美妆事业部'
                else null end as category_group,
            c.provider_code,
            b.goods_id,
            sum(b.original_goods_number) num,
            sum(b.original_goods_number*b.in_price_usd) sale_in_price
        from dw.dw_order_goods_fact b 
        inner join  dw.dw_order_fact a on a.order_id=b.order_id
        left join dim.dim_goods c on c.goods_id=b.goods_id
        left join jolly.who_esoloo_supplier  e on  c.provider_code = e.code
        left join dim.dim_goods_category_group_new as cat
        on c.cate_level1_name = cat.cate_level1_name
        where
        ((from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >= '20191001'
            and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') <= '20191031')
        or (from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >= '20191201'
            and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd')<= '20200131' ))
        and a.site_id  in(400,600,700,601,900) 
        and cat.category_group in ('beauty', '家居', '孕婴童用品', '婴童时尚')
        and a.pay_status in(1,3)
        and c.supplier_genre != 11
        group by 1,2,3
    ),

    supp_info as (
        select
            a0.business_division,
            a0.provider_code,
            sum(nvl(settle_total_amount_usd,0)) as settle_total_amount_usd
        from(
            select
                case when jsai.business_division=2 then '家居事业部'
                    when jsai.business_division=7 then '美妆事业部'
                    else null end as business_division,
                jsai.provider_code,
                jsai.settle_total_amount,
                jsai.currency,
                jsai.settle_rate,
                case when jsai.currency = "RMB" then jsai.settle_total_amount/7.0063
                    when jsai.currency = "SAR" then jsai.settle_total_amount*0.2664
                    when jsai.currency = "AED" then jsai.settle_total_amount*0.2723
                    else jsai.settle_total_amount end as settle_total_amount_usd
            from jolly_spm.jolly_spm_settle_accounts_info as jsai
            where jsai.business_division in (2, 7) -- '事业部：1.时尚事业部 2.家居事业部 3.3C事业部 4.MV事业部 5.快销事业部 6.家电建材事业部 7.美妆事业部'
            and settle_status in (0, 2)
            and jsai.settle_total_amount > 0
            and from_unixtime(jsai.gmt_created) >= '2019-06-01'
        ) as a0
        group by 1,2
    )


    select
        a0.category_group,
        a0.provider_code,
        a0.supp_name,
        a0.total_stock_num,
        a0.total_stock_price,
        a0.num,
        a0.sale_in_price,
        a0.total_stock_price/(a0.sale_in_price/3) as sale_month_can,
        nvl(suf.settle_total_amount_usd,0) as settle_total_amount_usd
    from (
        select
            si.category_group,
            si.provider_code,
            si.supp_name,
            sum(nvl(si.total_stock_num,0)) as total_stock_num,
            sum(nvl(si.total_stock_price,0)) as total_stock_price,
            sum(nvl(saf.num,0)) as num,
            sum(nvl(saf.sale_in_price,0)) as sale_in_price
        from stock_info as si
        left join sales_info as saf
        on si.category_group = saf.category_group
        and si.provider_code = saf.provider_code
        and si.goods_id = saf.goods_id
        group by 1,2,3
    ) as a0
    left join supp_info as suf
    on a0.category_group = suf.business_division
    and a0.provider_code = suf.provider_code
    """
    return sqlmsg


def new_goods_sale_sql():
    sqlmsg = """
    with sales as (
        select
            from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') data_date,
            b.goods_id,
            sum(b.original_goods_number) as num,
            sum(b.original_goods_number*b.goods_price) as revenue
        from dw.dw_order_goods_fact b 
        inner join  dw.dw_order_fact a on a.order_id=b.order_id
        left join dim.dim_goods c on c.goods_id=b.goods_id
        left join jolly.who_esoloo_supplier  e on  c.provider_code = e.code
        left join dim.dim_goods_category_group_new as cat
        on c.cate_level1_name = cat.cate_level1_name
        where from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >= from_timestamp(date_sub(now(),14),'yyyyMMdd')
        and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') <= from_timestamp(date_sub(now(),1),'yyyyMMdd')
        and a.site_id  in(400,600,700,601,900) 
        and a.pay_status in(1,3)
        and c.supplier_genre != 11
        and cat.category_group in ('家居','beauty', '大件家居', '孕婴童用品', '婴童时尚')
        group by 1,2
    ),

    goods as (
        select
            cat.category_group,
            case when dg.cate_level1_name = "Beauty" then dg.cate_level2_name else dg.cate_level1_name end as cate_level1_name,
            case when dg.cate_level1_name = "Beauty" then dg.cate_level3_name else dg.cate_level2_name end as cate_level2_name,
            case when dg.cate_level1_name = "Beauty" then dg.cate_level4_name else dg.cate_level3_name end as cate_level3_name,
            dg.goods_id,
            dg.goods_name,
            dg.first_on_sale_time,
            dge.in_price,
            dge.in_price_usd,
            dge.prst_price_1,
            dge.shop_price_1
        from dim.dim_goods as dg
        left join dim.dim_goods_extend as dge
        on dg.goods_id = dge.goods_id
        and dge.data_date = from_timestamp(date_sub(now(),1),'yyyyMMdd')
        left join dim.dim_goods_category_group_new as cat
        on dg.cate_level1_name = cat.cate_level1_name
        where cat.category_group in ('beauty','家居','孕婴童用品','婴童时尚')
        and dg.first_on_sale_time >= date_sub(to_date(now()),45)
        and dg.supplier_genre != 11
    ),

    stocks as (
        select
            gstd.goods_id,
            sum(gstd.total_stock_num) as total_stock_num,
            sum(case when gstd.depot_id not in (15,16, 36) then total_stock_num else 0 end) as cn_total_stock_num,
            sum(case when gstd.depot_id in (16, 36) then total_stock_num else 0 end) as sa_total_stock_num,
            sum(case when gstd.depot_id = 15 then total_stock_num else 0 end) as uae_total_stock_num,
            sum(gstd.free_stock_num) as free_stock_num,
            sum(case when gstd.depot_id not in (15,16, 36) then free_stock_num else 0 end) as cn_free_stock_num,
            sum(case when gstd.depot_id in (16, 36) then free_stock_num else 0 end) as sa_free_stock_num,
            sum(case when gstd.depot_id = 15 then free_stock_num else 0 end) as uae_free_stock_num
        from ods.ods_who_wms_goods_stock_total_detail as gstd
        left join dim.dim_goods as dg on gstd.goods_id = dg.goods_id
        left join dim.dim_goods_extend as dge 
        on dg.goods_id = dge.goods_id
        and gstd.data_date = dge.data_date
        left join dim.dim_goods_category_group_new as cat
        on dg.cate_level1_name = cat.cate_level1_name
        where cat.category_group in ('家居','beauty', '大件家居', '孕婴童用品', '婴童时尚')
        and gstd.data_date = from_timestamp(date_sub(now(),1), "yyyyMMdd")
        group by 1
    ),

    dau as (
        select
            dau.data_date,
            dau.goods_id,
            sum(dau.expo_uv) as expo_uv,
            sum(dau.goods_click_uv) as goods_click_uv,
            sum(dau.cart_click_uv) as cart_click_uv,
            sum(dau.bill_uv) as bill_uv,
            sum(dau.sales_uv) as sales_uv
        from rpt.rpt_sum_goods_daily as dau
        left join dim.dim_goods as dg
        on dau.goods_id = dg.goods_id
        left join dim.dim_goods_extend as dge
        on dg.goods_id = dge.goods_id
        and dau.data_date = dge.data_date
        left join dim.dim_goods_category_group_new as cat
        on dg.cate_level1_name = cat.cate_level1_name
        where dau.data_date >= from_timestamp(date_sub(now(),14), "yyyyMMdd")
        and dge.data_date <= from_timestamp(date_sub(now(),1), "yyyyMMdd")
        and dau.site_id in (400,600,700,601,900)
        and cat.category_group in ('家居','beauty', '大件家居', '孕婴童用品', '婴童时尚')
        group by 1,2
    )



    select
        goods.category_group as `类目组`,
        goods.cate_level1_name as `一级类目`,
        goods.cate_level2_name as `二级类目`,
        goods.cate_level3_name as `三级类目`,
        goods.goods_id as `goods_id`,
        goods.goods_name as `商品名称`,
        goods.first_on_sale_time as `上新时间`,
        goods.in_price as `成本(￥)`,
        goods.in_price_usd as `成本($)`,
        goods.prst_price_1 as `售价`,
        goods.shop_price_1 as `吊牌价`,
        stocks.total_stock_num as `总库存`,
        stocks.cn_total_stock_num as `国内库存`,
        stocks.sa_total_stock_num as `SA库存`,
        stocks.uae_total_stock_num as `UAE库存`,
        stocks.free_stock_num as `自由库存`,
        stocks.cn_free_stock_num as `国内自由库存`,
        stocks.sa_free_stock_num as `SA自由库存`,
        stocks.uae_free_stock_num as `UAE自由库存`,

        sum(case when sales.data_date >= from_timestamp(date_sub(now(),14),'yyyyMMdd')
                and sales.data_date < from_timestamp(date_sub(now(),7),'yyyyMMdd')
            then sales.num else 0 end) as `上周销量`,
        sum(case when sales.data_date >= from_timestamp(date_sub(now(),7),'yyyyMMdd')
                and sales.data_date <= from_timestamp(date_sub(now(),1),'yyyyMMdd')
            then sales.num else 0 end) as `本周销量`,
        sum(case when sales.data_date >= from_timestamp(date_sub(now(),14),'yyyyMMdd')
                and sales.data_date < from_timestamp(date_sub(now(),7),'yyyyMMdd')
            then sales.revenue else 0 end) as `上周GMV`,
        sum(case when sales.data_date >= from_timestamp(date_sub(now(),7),'yyyyMMdd')
                and sales.data_date <= from_timestamp(date_sub(now(),1),'yyyyMMdd')
            then sales.revenue else 0 end) as `本周GMV`,

        sum(case when sales.data_date >= from_timestamp(date_sub(now(),14),'yyyyMMdd')
                and sales.data_date < from_timestamp(date_sub(now(),7),'yyyyMMdd')
            then dau.expo_uv else 0 end) as `上周曝光uv`,
        sum(case when sales.data_date >= from_timestamp(date_sub(now(),7),'yyyyMMdd')
                and sales.data_date <= from_timestamp(date_sub(now(),1),'yyyyMMdd')
            then dau.expo_uv else 0 end) as `本周曝光uv`,

        sum(case when sales.data_date >= from_timestamp(date_sub(now(),14),'yyyyMMdd')
                and sales.data_date < from_timestamp(date_sub(now(),7),'yyyyMMdd')
            then dau.goods_click_uv else 0 end) as `上周点击uv`,
        sum(case when sales.data_date >= from_timestamp(date_sub(now(),7),'yyyyMMdd')
                and sales.data_date <= from_timestamp(date_sub(now(),1),'yyyyMMdd')
            then dau.goods_click_uv else 0 end) as `本周点击uv`,

        sum(case when sales.data_date >= from_timestamp(date_sub(now(),14),'yyyyMMdd')
                and sales.data_date < from_timestamp(date_sub(now(),7),'yyyyMMdd')
            then dau.cart_click_uv else 0 end) as `上周添加购物车uv`,
        sum(case when sales.data_date >= from_timestamp(date_sub(now(),7),'yyyyMMdd')
                and sales.data_date <= from_timestamp(date_sub(now(),1),'yyyyMMdd')
            then dau.cart_click_uv else 0 end) as `本周添加购物车uv`,

        sum(case when sales.data_date >= from_timestamp(date_sub(now(),14),'yyyyMMdd')
                and sales.data_date < from_timestamp(date_sub(now(),7),'yyyyMMdd')
            then dau.bill_uv else 0 end) as `上周下单uv`,
        sum(case when sales.data_date >= from_timestamp(date_sub(now(),7),'yyyyMMdd')
                and sales.data_date <= from_timestamp(date_sub(now(),1),'yyyyMMdd')
            then dau.bill_uv else 0 end) as `本周下单uv`,

        sum(case when sales.data_date >= from_timestamp(date_sub(now(),14),'yyyyMMdd')
                and sales.data_date < from_timestamp(date_sub(now(),7),'yyyyMMdd')
            then dau.sales_uv else 0 end) as `上周购买uv`,
        sum(case when sales.data_date >= from_timestamp(date_sub(now(),7),'yyyyMMdd')
                and sales.data_date <= from_timestamp(date_sub(now(),1),'yyyyMMdd')
            then dau.sales_uv else 0 end) as `本周购买uv`
    from goods left join sales
    on goods.goods_id = sales.goods_id
    left join stocks
    on goods.goods_id = stocks.goods_id
    left join dau
    on goods.goods_id = dau.goods_id
    and sales.data_date = dau.data_date
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
    """
    return sqlmsg


def gmv_day_report_sql():
    sqlmsg = """
    with sales as (
        select
            from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') data_date,
            cat.category_group,
            case when c.cate_level1_name="Beauty" then c.cate_level2_name else c.cate_level1_name end as cate_level1_name,
            case when c.supplier_genre = 11 then 1 else 0 end as is_pop,
            sum(b.original_goods_number) num,
            sum(b.original_goods_number*b.goods_price) revenue,
            sum(b.original_goods_number*b.in_price_usd) +nvl(sum( case when ((lower(a.country_name)='saudi arabia' and e.is_sa_supplier<>1) 
                or  (lower(a.country_name)='united arab emirates' and e.supplier_genre<>10)) then b.goods_price*b.original_goods_number*0.05
                when lower(a.country_name) in ('saudi arabia','united arab emirates')
                then  (b.goods_price-b.in_price_usd)*b.original_goods_number/1.05*0.05 end),0) cost_with_vat
        from dw.dw_order_goods_fact b 
        inner join  dw.dw_order_fact a on a.order_id=b.order_id
        left join dim.dim_goods c on c.goods_id=b.goods_id
        left join jolly.who_esoloo_supplier  e on  c.provider_code = e.code
        left join dim.dim_goods_category_group_new as cat
        on c.cate_level1_name = cat.cate_level1_name
        where
        ((from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >= from_timestamp(trunc(months_sub(date_sub(now(),1),1), "MM"),'yyyyMMdd')
            and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') <= from_timestamp(date_sub(now(),1),'yyyyMMdd'))
        or (from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >= from_timestamp(years_sub(trunc(months_sub(date_sub(now(),1),1), "MM"),1),'yyyyMMdd')
            and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd')<= from_timestamp(years_sub(date_sub(now(),1),1),'yyyyMMdd') ))
        and a.site_id  in(400,600,700,601,900) 
        and a.pay_status in(1,3)
        group by 1,2,3,4
    ),

    dau as (
        select
            cat.category_group,
            case when dg.cate_level1_name="Beauty" then dg.cate_level2_name else dg.cate_level1_name end as cate_level1_name,
            dau.data_date,
            case when dg.supplier_genre = 11 then 1 else 0 end as is_POP,
            sum(dau.expo_pv) as expo_pv,
            sum(dau.expo_uv) as expo_uv,
            sum(dau.goods_click_uv) as goods_click_uv,
            sum(dau.cart_click_uv) as car_click_uv,
            sum(dau.bill_uv) as bill_uv,
            sum(dau.bill_uv)/sum(dau.goods_click_uv) as click_bill,
            sum(dau.sales_uv) as sales_uv,
            sum(dau.sales_uv)/sum(dau.goods_click_uv) as click_sale
        from rpt.rpt_sum_goods_daily as dau
        left join dim.dim_goods as dg
        on dau.goods_id = dg.goods_id
        left join dim.dim_goods_category_group_new as cat
        on dg.cate_level1_name = cat.cate_level1_name
        where dau.data_date >= from_timestamp(trunc(months_sub(date_sub(now(),1),1), "MM"),'yyyyMMdd')
        and dau.data_date <= from_timestamp(date_sub(now(),1),'yyyyMMdd')
        and dau.site_id in (400,600,700,601,900)
        group by 1,2,3,4
    ),

    income as (
        select  --当月使用预测净利润
        regexp_replace(pay_date,'-','') data_date
        ,cat.category_group
        ,case when g.cate_level1_name = "Beauty" then g.cate_level2_name else g.cate_level1_name end as cate_level1_name
        ,sum(profits2) pre_income
        from zybiro.bi_faye_net_profit_precast_new a
        left join dim.dim_goods g on a.goods_id=g.goods_id
        left join dim.dim_goods_category_group_new as cat
        on g.cate_level1_name = cat.cate_level1_name
        where data_date = regexp_replace(to_date(date_sub(current_timestamp(),1)),'-','')
        and regexp_replace(pay_date,'-','') = from_timestamp(date_sub(now(),1),'yyyyMMdd')
        and cat.category_group in ('家居','beauty', '大件家居', '孕婴童用品', '婴童时尚')
        group by 1,2,3
        union
        select 
        regexp_replace(substr(case when b.pay_id=41 then b.pay_time else b.result_pay_time end,1,10 ),'-','') data_date,
        cat.category_group,
        case when p2.cate_level1_name = "Beauty" then p2.cate_level2_name else p2.cate_level1_name end as cate_level1_name,
        sum(a.income)+sum(a.discountamount)-sum(a.cost)-sum(a.newshippingfees)-sum(a.thedepotfees)-sum(vat)-sum(a.duty) pre_income-- 净利额
        from zybiro.bi_damon_netprofit_2018 a  -- 统一采用damon备份表，下午才能更新
        inner join dw.dw_order_sub_order_fact b
        on a.order_id=b.order_id
        inner join dim.dim_goods p2
        on a.goods_id=p2.goods_id
        left join dim.dim_goods_category_group_new as cat
        on p2.cate_level1_name = cat.cate_level1_name
        where ((regexp_replace(substr(case when b.pay_id=41 then b.pay_time else b.result_pay_time end,1,10 ),'-','')>=from_timestamp(months_sub(trunc(to_date(now()),"MM"),1),'yyyyMMdd')
            and regexp_replace(substr(case when b.pay_id=41 then b.pay_time else b.result_pay_time end,1,10 ),'-','')< from_timestamp(date_sub(now(),1),'yyyyMMdd'))
        or (regexp_replace(substr(case when b.pay_id=41 then b.pay_time else b.result_pay_time end,1,10 ),'-','')>=from_timestamp(years_sub(months_sub(trunc(to_date(now()),"MM"),1),1),'yyyyMMdd')
            and regexp_replace(substr(case when b.pay_id=41 then b.pay_time else b.result_pay_time end,1,10 ),'-','')< from_timestamp(years_sub(date_sub(now(),1),1),'yyyyMMdd')))
        and b.site_id in (400,700,600,900,601)
        and b.pay_status in(1,3)
        and p2.supplier_genre<>11  -- 剔除pop供应商
        and cat.category_group in ('家居','beauty', '大件家居', '孕婴童用品', '婴童时尚')
        group by 
        1,2,3
    ),

    base as (
        select
            sales.category_group,
            sales.cate_level1_name,
            sales.data_date,
            sales.is_pop,
            sales.num,
            sales.revenue,
            sales.cost_with_vat,
            income.pre_income as j_income,
            dau.expo_pv,
            dau.expo_uv,
            dau.goods_click_uv,
            dau.car_click_uv,
            dau.bill_uv,
            dau.sales_uv
        from sales left join dau
        on sales.category_group = dau.category_group
        and sales.cate_level1_name = dau.cate_level1_name
        and sales.data_date = dau.data_date
        and sales.is_pop = dau.is_pop
        left join income
        on sales.category_group = income.category_group
        and sales.cate_level1_name = income.cate_level1_name
        and sales.data_date = income.data_date
        and sales.is_pop = 0
    )


    select
        case when base.category_group in ('家居','婚庆周边')                                             then '家居'
            when base.category_group in ('婴童时尚','孕婴童用品')                                        then '母婴童'
            when base.category_group in ('女装','服装2（内衣）','鞋包','ACC','男装',"包", "鞋",'Sports') then '时尚'
            when base.category_group in ('数码', '车品','Appliances','Computers & Accessories')          then '3C'
            when base.category_group in ('手机')                                                         then '手机'
            when base.category_group in ('beauty')                                                       then '美妆' 
            when base.category_group in ('快消')                                                         then '执御超市'
            when base.cate_level1_name in ("Baby", "Mom")                                                then "母婴童"
            when base.cate_level1_name in ("Men's Accessories", "Women's Accessories")                   then "时尚"
            when base.cate_level1_name = "Reload Card"                                                   then "时尚"
            else '其他' end                                                                              as `BU`
        ,cg.department
        ,base.category_group
        ,base.cate_level1_name
        ,base.data_date
        ,base.is_pop
        ,cast(substr(base.data_date,1,4) as int) as years0
        ,cast(substr(base.data_date,5,2) as int) as month0
        ,sum(base.num)                                                                        as `销量`
        ,round(sum(revenue),2)                                                                as `GMV`
        ,round(sum(j_income),2)                                                               as `净利润`
        ,round(sum(cost_with_vat),2)                                                          as `成本`
        ,(sum(base.revenue) - sum(base.cost_with_vat))/sum(base.revenue)                      as `毛利率`
        ,sum(base.expo_uv)                                                                    as `曝光uv`
        ,sum(base.expo_pv)                                                                    as `曝光pv`
        ,sum(base.goods_click_uv)                                                             as `点击uv`
        ,sum(base.car_click_uv)                                                               as `加购uv`
        ,sum(base.bill_uv)                                                                    as `下单uv`
        ,sum(base.sales_uv)                                                                   as `购买uv`
    from base
    left join zybiro.bi_longjy_category_group_new as cg
    on base.cate_level1_name = cg.cate_level1_name
    where ((data_date >= from_timestamp(trunc(months_sub(date_sub(now(),1),1), "MM"),'yyyyMMdd')
    and data_date <= from_timestamp(date_sub(now(),1),'yyyyMMdd'))
    or (data_date >= from_timestamp(years_sub(trunc(months_sub(date_sub(now(),1),1), "MM"),1),'yyyyMMdd')
    and data_date <= from_timestamp(years_sub(date_sub(now(),1),1),'yyyyMMdd')))
    and base.category_group != '快消'
    and base.category_group != '废弃'
    group by 1,2,3,4,5,6
    """
    return sqlmsg


def on_sale_goods_day_sql():
    sqlmsg = """
    select
        cat.category_group,
        case when cat.category_group = "beauty" then dg.cate_level2_name else dg.cate_level1_name end as cate_level1_name,
        substr(dge.data_date, 1,6) as years0,
        dge.data_date,
        count(distinct dg.goods_id) as goods_num
    from dim.dim_goods as dg
    left join dim.dim_goods_extend as dge
    on dg.goods_id = dge.goods_id
    left join dim.dim_goods_category_group_new as cat
    on dg.cate_level1_name = cat.cate_level1_name
    where cat.category_group in ('家居','beauty', '大件家居', '孕婴童用品', '婴童时尚')
    and dg.supplier_genre != 11
    and dge.data_date >= from_timestamp(date_sub(now(),60),'yyyyMMdd') and dge.data_date <= from_timestamp(date_sub(now(),1),'yyyyMMdd')
    and dge.is_jc_on_sale=1
    group by 1,2,3,4
    """
    return sqlmsg


def new_goods_day_sql():
    sqlmsg = """
    select
        cat.category_group,
        case when cat.category_group = "beauty" then dg.cate_level2_name else dg.cate_level1_name end as cate_level1_name,
        from_timestamp(to_date(dg.first_on_sale_time),'yyyy-MM-dd') as data_date,
        count(distinct dg.goods_id) as goods_num
    from dim.dim_goods as dg
    left join dim.dim_goods_category_group_new as cat
    on dg.cate_level1_name = cat.cate_level1_name
    where cat.category_group in ('家居','beauty', '大件家居', '孕婴童用品', '婴童时尚')
    and dg.supplier_genre != 11
    and dg.first_on_sale_time >= to_date(months_sub(trunc(date_sub(now(),1), "MM"),1))
    and dg.first_on_sale_time <= to_date(now())
    group by 1,2,3
    """
    return sqlmsg


def tar_goods_price_gmv_duibi():
    sqlmsg = """
    with goods_info as (
        select
            case when dg.cate_level1_name = "Beauty" then dg.cate_level2_name else dg.cate_level1_name end as cate_level1_name,
            case when dg.cate_level1_name = "Beauty" then dg.cate_level3_name else dg.cate_level2_name end as cate_level2_name,
            case when dg.cate_level1_name = "Beauty" then dg.cate_level4_name else dg.cate_level3_name end as cate_level3_name,
            dg.goods_id,
            dg.goods_name
        from dim.dim_goods as dg
        left join zybiro.bi_longjy_goods_price_tmp as gt
        on dg.goods_id = gt.goods_id
        where gt.goods_id is not null
    ),

    dau_info as (
        select
            dau.data_date,
            dg.goods_id,
            sum(dau.goods_click_uv) as goods_click_uv,
            sum(dau.bill_uv) as bill_uv,
            sum(dau.sales_uv) as sales_uv
        from rpt.rpt_sum_goods_daily as dau
        left join dim.dim_goods as dg
        on dau.goods_id = dg.goods_id
        left join zybiro.bi_longjy_goods_price_tmp as gt
        on dg.goods_id = gt.goods_id
        where dau.data_date >= '20200312'
        and dau.data_date <= '20200326'
        and dau.site_id in (400,600,700,601,900)
        and dg.supplier_genre != 11
        and gt.goods_id is not null
        group by 1,2
    ),

    sales_info as (
        select
            from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') data_date,
            b.goods_id,
            sum(b.original_goods_number) num,
            sum(b.original_goods_number*b.goods_price) revenue
        from dw.dw_order_goods_fact b 
        inner join  dw.dw_order_fact a on a.order_id=b.order_id
        left join dim.dim_goods c on c.goods_id=b.goods_id
        left join jolly.who_esoloo_supplier  e on  c.provider_code = e.code
        left join zybiro.bi_longjy_goods_price_tmp as gt
        on c.goods_id = gt.goods_id
        where
        from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >= '20200312'
        and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') <= '20200326'
        and a.site_id  in(400,600,700,601,900) 
        and a.pay_status in(1,3)
        and c.supplier_genre != 11
        and gt.goods_id is not null
        group by 1,2
    )



    select
        a0.cate_level1_name,
        a0.cate_level2_name,
        a0.cate_level3_name,
        a0.goods_id,
        a0.goods_name,
        sum(case when a0.data_date >= "20200312" and a0.data_date<="20200318" then a0.goods_click_uv else 0 end) as goods_click_uv_befor,
        sum(case when a0.data_date >= "20200312" and a0.data_date<="20200318" then a0.bill_uv else 0 end) as bill_uv_befor,
        sum(case when a0.data_date >= "20200312" and a0.data_date<="20200318" then a0.sales_uv else 0 end) as sales_uv_befor,
        sum(case when a0.data_date >= "20200312" and a0.data_date<="20200318" then a0.num else 0 end) as num_befor,
        sum(case when a0.data_date >= "20200312" and a0.data_date<="20200318" then a0.revenue else 0 end) as revenue_befor,
        sum(case when a0.data_date >= "20200319" and a0.data_date<="20200326" then a0.goods_click_uv else 0 end) as goods_click_uv_after,
        sum(case when a0.data_date >= "20200319" and a0.data_date<="20200326" then a0.bill_uv else 0 end) as bill_uv_after,
        sum(case when a0.data_date >= "20200319" and a0.data_date<="20200326" then a0.sales_uv else 0 end) as sales_uv_after,
        sum(case when a0.data_date >= "20200319" and a0.data_date<="20200326" then a0.num else 0 end) as num_after,
        sum(case when a0.data_date >= "20200319" and a0.data_date<="20200326" then a0.revenue else 0 end) as revenue_after
    from (
        select
            gi.cate_level1_name,
            gi.cate_level2_name,
            gi.cate_level3_name,
            gi.goods_id,
            gi.goods_name,
            di.data_date,
            di.goods_click_uv,
            di.bill_uv,
            di.sales_uv,
            si.num,
            si.revenue
        from goods_info as gi
        left join dau_info as di
        on gi.goods_id = gi.goods_id
        left join sales_info as si
        on di.goods_id = si.goods_id
        and di.data_date = si.data_date
    ) as a0
    group by 1,2,3,4,5
    """
    return sqlmsg


def supp_off_goods_sql():
    sqlmsg = """
    with sales_info as (
        select
            a0.category_group,
            a0.cate_level1_name,
            a0.provider_code,
            a0.goods_id,
            a0.sku_id,
            a0.num,
            a0.revenue
        from(
            select
                cat.category_group,
                case when c.cate_level1_name="Beauty" then c.cate_level2_name else c.cate_level1_name end as cate_level1_name,
                b.goods_id,
                b.sku_id,
                c.provider_code,
                sum(b.original_goods_number) num,
                sum(b.original_goods_number*b.goods_price) revenue
            from dw.dw_order_goods_fact b 
            inner join  dw.dw_order_fact a on a.order_id=b.order_id
            left join dim.dim_goods c on c.goods_id=b.goods_id
            left join jolly.who_esoloo_supplier  e on  c.provider_code = e.code
            left join dim.dim_goods_category_group_new as cat
            on c.cate_level1_name = cat.cate_level1_name
            where cat.category_group in ('家居','beauty', '孕婴童用品', '婴童时尚')
            and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >= from_timestamp(date_sub(now(),7),'yyyyMMdd')
            and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') <= from_timestamp(date_sub(now(),1),'yyyyMMdd')
            and a.site_id  in(400,600,700,601,900) 
            and a.pay_status in(1,3)
            and c.supplier_genre != 11
            group by 1,2,3,4,5
        ) as a0
        where a0.num > = 5
    ),

    off_info as (
        select
            off_log.goods_id,
            off_log.sku_id,
            off_log.supp_code,
            off_log.status,
            from_unixtime(off_log.gmt_created) as gmt_created,
            off_log.depot_coverage_area_id,
            depot_aree.area_name,
            off_log.admin_id
        from jolly.who_wms_sku_on_off_log as off_log
        left join dim.dim_goods as dg
        on off_log.goods_id = dg.goods_id
        left join dim.dim_goods_category_group_new as cat
        on dg.cate_level1_name = cat.cate_level1_name
        left join jolly.who_shipping_depot_coverage_area as depot_aree
        on off_log.depot_coverage_area_id = depot_aree.depot_coverage_area_id
        where cat.category_group in ('家居','beauty', '孕婴童用品', '婴童时尚')
        and from_timestamp(from_unixtime(off_log.gmt_created), 'yyyyMMdd') = from_timestamp(date_sub(now(),1),'yyyyMMdd')
        and off_log.status = 1
        and dg.supplier_genre != 11
        
    )


    select
        si.category_group as `类目组`,
        si.cate_level1_name as `一级类目`,
        si.goods_id,
        si.sku_id,
        si.num as `近7天销量`,
        si.revenue as `近7天GMV`,
        oi.supp_code as `供应商编码`,
        oi.status as `是否下降`,
        oi.gmt_created as `操作时间`,
        oi.area_name as `下架区域`
    from sales_info as si
    left join off_info as oi
    on si.goods_id = oi.goods_id
    and si.sku_id = oi.sku_id
    and si.provider_code = oi.supp_code
    where oi.goods_id is not null
    order by si.category_group,
        si.cate_level1_name,
        si.goods_id,
        si.sku_id;
    """
    return sqlmsg

def sa_depot_goods_sale_num():
    sqlmsg = """
    select
        cat.category_group,
        dg.cate_level1_name,
        to_date(case when a.pay_id=41 then a.pay_time else a.result_pay_time end) data_date,
        case when a.depod_id = 16 then '沙特'
            when a.depod_id = 15 then '迪拜'
            else '国内' end as depod,
        b.goods_id,
        case when dg.supplier_genre in (6,9,10) then '本地供应商' else '国内供应商' end as supplier_type,
        sum(b.original_goods_number) as num,
        sum(b.original_goods_number*b.goods_price) as gmv
    from dw.dw_order_sub_order_fact as a
    left join dw.dw_order_goods_fact b on a.order_id = b.order_id
    left join dim.dim_goods as dg on b.goods_id = dg.goods_id
    left join dim.dim_goods_category_group_new as cat
    on dg.cate_level1_name = cat.cate_level1_name
    where from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd')>=from_timestamp(date_sub(now(),7),'yyyyMMdd')
    and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd')<=from_timestamp(date_sub(now(),1),'yyyyMMdd')
    and cat.category_group in ('家居', '婴童时尚','孕婴童用品', 'beauty')
    and dg.supplier_genre <> 11
    and a.depod_id = 16
    and a.site_id in (400,600,700,601,900)
    group by 1,2,3,4,5,6
    """
    return sqlmsg

def home_3_sku_sale():
    sqlmsg = """
    select
        from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') data_date,
        c.cate_level1_name,
        c.goods_id,
        b.sku_id,
        sum(b.original_goods_number) num
    from dw.dw_order_goods_fact b 
    inner join  dw.dw_order_fact a on a.order_id=b.order_id
    left join dim.dim_goods c on c.goods_id=b.goods_id
    left join jolly.who_esoloo_supplier  e on  c.provider_code = e.code
    left join dim.dim_goods_category_group_new as cat
    on c.cate_level1_name = cat.cate_level1_name
    where
    from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >= '20200301'
    and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') <= '20200331'
    and cat.category_group =  '家居'
    and a.site_id  in(400,600,700,601,900) 
    and a.pay_status in(1,3)
    and c.supplier_genre != 11
    group by 1,2,3,4
    """
    return sqlmsg

def gmv_duibi():
    sqlmsg = """
       with sales as (
        select
            from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') data_date,
            cg.department,
            cat.category_group,
            case when c.cate_level1_name="Beauty" then c.cate_level2_name else c.cate_level1_name end as cate_level1_name,
            case when c.supplier_genre = 11 then 1 else 0 end as is_pop,
            b.goods_id,
            sum(b.original_goods_number) num,
            sum(b.original_goods_number*b.goods_price) revenue,
            sum(a.order_amount_no_bonus) total_amount,
            sum(b.original_goods_number*b.in_price_usd) +nvl(sum( case when ((lower(a.country_name)='saudi arabia' and e.is_sa_supplier<>1) 
                or  (lower(a.country_name)='united arab emirates' and e.supplier_genre<>10)) then b.goods_price*b.original_goods_number*0.05
                when lower(a.country_name) in ('saudi arabia','united arab emirates')
                then  (b.goods_price-b.in_price_usd)*b.original_goods_number/1.05*0.05 end),0) cost_with_vat
        from dw.dw_order_goods_fact b 
        inner join  dw.dw_order_fact a on a.order_id=b.order_id
        left join dim.dim_goods c on c.goods_id=b.goods_id
        left join jolly.who_esoloo_supplier  e on  c.provider_code = e.code
        left join dim.dim_goods_category_group_new as cat
        on c.cate_level1_name = cat.cate_level1_name
        left join zybiro.bi_longjy_category_group_new as cg
        on c.cate_level1_name = cg.cate_level1_name
        where
        from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >= '20200501'
        and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') <= '20205009'
        and a.site_id  in(400,600,700,601,900) 
        and a.pay_status in(1,3)
        group by 1,2,3,4,5,6
    ),

    dau as (
        select
            dau.data_date,
            dau.goods_id,
            sum(dau.expo_pv) as expo_pv,
            sum(dau.expo_uv) as expo_uv,
            sum(dau.goods_click_uv) as goods_click_uv,
            sum(dau.bill_uv) as bill_uv,
            sum(dau.bill_uv)/sum(dau.goods_click_uv) as click_bill,
            sum(dau.sales_uv) as sales_uv,
            sum(dau.sales_uv)/sum(dau.goods_click_uv) as click_sale
        from rpt.rpt_sum_goods_daily as dau
        left join dim.dim_goods as dg
        on dau.goods_id = dg.goods_id
        left join dim.dim_goods_category_group_new as cat
        on dg.cate_level1_name = cat.cate_level1_name
        where dau.data_date >= '20200501'
        and dau.data_date <= '20200509'
        and dau.site_id in (400,600,700,601,900)
        group by 1,2
    )


    select
        sales.department,
        sales.category_group,
        sales.cate_level1_name,
        sales.data_date,
        sales.is_pop,
        sales.num,
        sales.revenue,
        sales.total_amount,
        sales.cost_with_vat,
        dau.expo_pv,
        dau.expo_uv,
        dau.goods_click_uv,
        dau.bill_uv,
        dau.sales_uv
    from sales left join dau
    on sales.goods_id = dau.goods_id
    and sales.data_date = dau.data_date
    """
    return sqlmsg


def GMV_department_6_gmv_tar_rate():
    sql_msg = """
        with sales as (
            select
                from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') data_date,
                cg.department,
                case when c.cate_level1_name="Beauty" then c.cate_level2_name else c.cate_level1_name end as cate_level1_name,
                case when c.cate_level1_name="Beauty" then c.cate_level3_name else c.cate_level2_name end as cate_level2_name,
                case when c.supplier_genre = 11 then 1 else 0 end as is_pop,
                sum(b.original_goods_number) num,
                sum(b.original_goods_number*b.goods_price) revenue,
                sum(b.original_goods_number*b.in_price_usd) +nvl(sum( case when ((lower(a.country_name)='saudi arabia' and e.is_sa_supplier<>1) 
                    or  (lower(a.country_name)='united arab emirates' and e.supplier_genre<>10)) then b.goods_price*b.original_goods_number*0.05
                    when lower(a.country_name) in ('saudi arabia','united arab emirates')
                    then  (b.goods_price-b.in_price_usd)*b.original_goods_number/1.05*0.05 end),0) cost_with_vat
            from dw.dw_order_goods_fact b 
            inner join  dw.dw_order_fact a on a.order_id=b.order_id
            left join dim.dim_goods c on c.goods_id=b.goods_id
            left join jolly.who_esoloo_supplier  e on  c.provider_code = e.code
            left join dim.dim_goods_category_group_new as cat
            on c.cate_level1_name = cat.cate_level1_name
            left join zybiro.bi_longjy_category_group_new as cg
            on c.cate_level1_name = cg.cate_level1_name
            where
            from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') >= from_timestamp(trunc(to_date(date_sub(now(),1)),"MM"),'yyyyMMdd')
            and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') <= from_timestamp(date_sub(now(),1),'yyyyMMdd')
            and cg.department = "六部"
            and a.site_id  in(400,600,700,601,900) 
            and a.pay_status in(1,3)
            group by 1,2,3,4,5
        ),

        income as (
            select  --当月使用预测净利润
                regexp_replace(pay_date,'-','') data_date
                ,cg.department
                ,case when g.cate_level1_name = "Beauty" then g.cate_level2_name else g.cate_level1_name end as cate_level1_name
                ,case when g.cate_level1_name = "Beauty" then g.cate_level3_name else g.cate_level2_name end as cate_level2_name
                ,sum(profits2) pre_income
            from zybiro.bi_faye_net_profit_precast_new a
            left join dim.dim_goods g on a.goods_id=g.goods_id
            left join dim.dim_goods_category_group_new as cat
            on g.cate_level1_name = cat.cate_level1_name
            left join zybiro.bi_longjy_category_group_new as cg
            on g.cate_level1_name = cg.cate_level1_name
            where data_date = regexp_replace(to_date(date_sub(current_timestamp(),1)),'-','')
            and regexp_replace(pay_date,'-','') = from_timestamp(date_sub(now(),1),'yyyyMMdd')
            and cg.department = "六部"
            group by 1,2,3,4
            union
            select 
                regexp_replace(substr(case when b.pay_id=41 then b.pay_time else b.result_pay_time end,1,10 ),'-','') data_date,
                cg.department,
                case when p2.cate_level1_name = "Beauty" then p2.cate_level2_name else p2.cate_level1_name end as cate_level1_name,
                case when p2.cate_level1_name = "Beauty" then p2.cate_level3_name else p2.cate_level2_name end as cate_level2_name,
                sum(a.income)+sum(a.discountamount)-sum(a.cost)-sum(a.newshippingfees)-sum(a.thedepotfees)-sum(vat)-sum(a.duty) pre_income-- 净利额
            from zybiro.bi_damon_netprofit_2018 a  -- 统一采用damon备份表，下午才能更新
            inner join dw.dw_order_sub_order_fact b
            on a.order_id=b.order_id
            inner join dim.dim_goods p2
            on a.goods_id=p2.goods_id
            left join dim.dim_goods_category_group_new as cat
            on p2.cate_level1_name = cat.cate_level1_name
            left join zybiro.bi_longjy_category_group_new as cg
            on p2.cate_level1_name = cg.cate_level1_name
            where ((regexp_replace(substr(case when b.pay_id=41 then b.pay_time else b.result_pay_time end,1,10 ),'-','')>=from_timestamp(months_sub(trunc(to_date(now()),"MM"),1),'yyyyMMdd')
                and regexp_replace(substr(case when b.pay_id=41 then b.pay_time else b.result_pay_time end,1,10 ),'-','')< from_timestamp(date_sub(now(),1),'yyyyMMdd'))
            or (regexp_replace(substr(case when b.pay_id=41 then b.pay_time else b.result_pay_time end,1,10 ),'-','')>=from_timestamp(years_sub(months_sub(trunc(to_date(now()),"MM"),1),1),'yyyyMMdd')
                and regexp_replace(substr(case when b.pay_id=41 then b.pay_time else b.result_pay_time end,1,10 ),'-','')< from_timestamp(years_sub(date_sub(now(),1),1),'yyyyMMdd')))
            and b.site_id in (400,700,600,900,601)
            and b.pay_status in(1,3)
            and cg.department = "六部"
            and p2.supplier_genre<>11  -- 剔除pop供应商
            group by 
            1,2,3,4
        )


        select
            sales.department,
            sales.cate_level1_name,
            sales.cate_level2_name,
            sales.data_date,
            sales.is_pop,
            sales.num,
            sales.revenue,
            sales.cost_with_vat,
            income.pre_income
        from sales left join income
        on sales.department = income.department
        and sales.cate_level1_name = income.cate_level1_name
        and sales.cate_level2_name = income.cate_level2_name
        and sales.data_date = income.data_date
        and sales.is_pop = 0
        """
    return sql_msg


def flash_week_home():
    sqlmsg = """
    with on_sale as  ---商品在架且能售
    (
    select 
    goods_id 
    from 
    dim.dim_goods_extend 
    where data_date = regexp_replace(to_date(date_sub(current_timestamp(),1)),'-','')
    and is_jc_on_sale = 1 
    and is_jc_sale =1 
    --and is_jc_enable = 1
    ),
    bi_price as  ---bi针对不同人群设定的最高价格 SA地区
    (
    select 
    goods_id ,
    sku_id,
    max(case when currency ='USD' then bi_price
            when currency ='QAR' then bi_price/3.64
            when currency ='SAR' then bi_price/3.75 
            when currency ='KWD' then bi_price/0.3036
            when currency ='AED' then bi_price/3.6728
            end)  bi_price
    from 
    ods.ods_bi_sell_price_sku 
    where 
    data_date = regexp_replace(to_date(date_sub(current_timestamp(),1)),'-','')--regexp_replace(to_date(current_timestamp()),'-','')
    and country = 'SA' 
    group by 1,2
    ),
    Break_price as ----jc的盈亏平衡价
    (
    select 
    rec_id sku_id,
    case when price1!=0 and price1 is not null then price1 else price2 end Break_price----盈亏平衡价
    from zybiro.tmp_faye_fashion_balance_price_lc30_new a 
    where data_date=regexp_replace(to_date(date_sub(current_timestamp(),1)),'-','')
    and site = 'jc'
    ),
    promote_price as  ----价格分默认和其他区域，暂取默认区域
    (
    select 
    sku_id ,
    goods_id,
    from_unixtime(promote_start_time,'yyyy-MM-dd') promote_start_time,
    from_unixtime(promote_end_time,'yyyy-MM-dd') promote_end_time,
    shop_price,  --商品吊牌
    promote_price  ---促销价
    from 
    jolly.who_sku_sell_price 
    where area_id = 1 
    ),
    conversion_funnel as 
    (
    select   --商品的 月销售数量、收入、吊牌、成本、曝光、点击、加购、收藏、 均价
    ws.goods_id
    ,sum(7d_sales_volume             )                       num_7day
    ,sum(7d_sales_amount             )                   revenue_7day
    ,sum(7d_sales_amount )/sum(7d_expo_pv)*1000         pv_value_7day
    ,sum(7d_cart_click )/sum(7d_goods_click)            click_to_cart_7day
    ,sum(30d_sales_volume)                              num_30day
    ,sum(30d_sales_amount             )                   revenue_30day
    ,sum(30d_sales_amount )/sum(30d_expo_pv)*1000           pv_value_30day
    ,sum(30d_goods_click )/sum(30d_expo_pv)         pv_to_click_30day
    from
    rpt.rpt_sum_goods_daily ws
    where
    ws.data_date = regexp_replace(to_date(date_sub(current_timestamp(),1)),'-','')
    and
    ws.site_id  in(400,600,700,601,900) 
    group by
    ws.goods_id
    ),
    ------该商品是否单一成本
    singe_inprice as 
    (
    select 
    goods_id,
    case when max(in_price)=min(in_price) then '单成本' else '多成本' end singe_inprice
    from 
    jolly.who_sku_relation 
    where status = 0
    group by goods_id
    ),
    singe_shop_price as  ----价格分默认和其他区域，暂取默认区域
    (
    select 
    ws.goods_id,
    case when max(shop_price)= min(shop_price) then '单吊牌价' else '多吊牌价' end singe_shop_price
    from 
    jolly.who_sku_sell_price  ws
    left join jolly.who_sku_relation  ws1 on ws.sku_id = ws1.rec_id
    where ws.area_id = 1 
    and ws1.status = 0
    group by goods_id 
    )
    
    select 
    *
    from  
        (
        ----选出商品合适的商品（1、单吊牌的商品能统一定价   2、成本价差异不超过1RMB的可以统一调整吊牌价 ） 
        select 
        case when singe_shop_price = '单吊牌价' and  singe_inprice = '单成本'  then 'goods定价' 
            when singe_shop_price = '单吊牌价' and  singe_inprice = '多成本'  then '供应商调价'
            else 'SKU定价' end '定价的维度'
        ,ws.goods_id
        ,ws.sku_id
        ,ws.BU
        ,ws.category_group
        ,ws.cate_level1_name
        ,ws.cate_level2_name
        ,ws.cate_level3_name
        ,ws.shop_price
        ,ws.in_price
        ,ws.price_now
        ,ws.price_now*0.82 'flash_price'
        ,(ws.price_now*0.82 - ws.in_price/7)/(ws.price_now*0.82)  'maolilv' 
        ,(ws.price_now*0.82 - Break_price)/(ws.price_now*0.82)  'jinglilv'  
        ,1-(ws.price_now*0.82/ws.shop_price) 'zhekou'   
        ,ws.price_now*0.82 - ws.price_now 'bijia'   
        ,500  'kucun'
        ,ws.first_on_sale_time
        ,ws.click_to_cart_7day
        ,ws.num_7day
        ,ws.revenue_7day
        ,ws.pv_value_7day
        ,ws.num_30day
        ,ws.revenue_30day
        ,Break_price
        from 
            (
            select 
            ws.*
            ,row_number() over (partition by ws.goods_id order by ws.in_price desc) in_price_rank
            from 
                (
                select 
                ws.rec_id  sku_id ,
                ws.goods_id,
                ws5.goods_name,
                case when ws5.category_group = 'beauty' then '美妆事业部'  else '家居母婴童事业部' end 'BU',
                ws5.cate_level1_name,
                ws5.cate_level2_name,
                ws5.cate_level3_name,
                ws4.shop_price , 
                ws.in_price   ,
                case when ws2.sku_id is not null  then ws2.bi_price 
                     when ws.promote_price= 0 then ws4.shop_price else ws.promote_price end  price_now,
                ws9.singe_inprice,
                ws10.singe_shop_price,
                ws5.provider_code,
                to_date(ws5.first_on_sale_time) first_on_sale_time,
                ws5.category_group,
                ws3.Break_price ,
                case when ws2.sku_id is null then '非智能' else '智能定价' end '是否智能定价',
                ws7.num_7day,
                ws7.revenue_7day,
                ws7.pv_value_7day,
                ws7.click_to_cart_7day,
                ws7.num_30day,
                ws7.revenue_30day,
                ws7.pv_value_30day,
                ws7.pv_to_click_30day
                from 
                jolly.who_sku_relation  ws
                left join on_sale  ws1 on ws.goods_id =ws1.goods_id 
                left join bi_price ws2 on ws.rec_id = ws2.sku_id
                left join Break_price ws3 on ws.rec_id = ws3.sku_id
                left join promote_price ws4 on ws.rec_id = ws4.sku_id
                left join dim.dim_goods ws5 on ws.goods_id = ws5.goods_id 
                left join conversion_funnel ws7 on ws.goods_id = ws7.goods_id 
                left join singe_inprice ws9 on ws.goods_id = ws9.goods_id
                left join singe_shop_price ws10 on ws.goods_id = ws10.goods_id
                where ws.status= 0
                and ws1.goods_id is not null 
                and ws5.category_group = '家居'
                -- and ws5.category_group in ('婴童时尚',  '孕婴童用品') --,'家居','beauty') --in ('Home Linens' 'Event and Party Supplies','Home Supplies')  ---'Home Linens'  --Pet Supplies Home Decor  --ws5.cate_level1_name = 'Fragrances'  
                and ws5.supplier_genre  in (1,2)
                --and ws5.provider_code = 'A34'
                and ws7.num_30day > 0 
                ) ws 
            )ws 
            where  ( (singe_shop_price = '单吊牌价'  and in_price_rank = 1)  or singe_shop_price ='多吊牌价') 
        )ws 
    where ws.maolilv >= 0.45  and jinglilv >= -0.02 and zhekou >= 0.25
        and ((flash_price<=3.99 and num_7day >= 65) or   (flash_price<=5.99 and num_7day >= 6)  or flash_price>5.99)
    """
    return sqlmsg


def flash_week_kids():
    sqlmsg = """
    with on_sale as  ---商品在架且能售
    (
    select 
    goods_id 
    from 
    dim.dim_goods_extend 
    where data_date = regexp_replace(to_date(date_sub(current_timestamp(),1)),'-','')
    and is_jc_on_sale = 1 
    and is_jc_sale =1 
    --and is_jc_enable = 1
    ),
    bi_price as  ---bi针对不同人群设定的最高价格 SA地区
    (
    select 
    goods_id ,
    sku_id,
    max(case when currency ='USD' then bi_price
            when currency ='QAR' then bi_price/3.64
            when currency ='SAR' then bi_price/3.75 
            when currency ='KWD' then bi_price/0.3036
            when currency ='AED' then bi_price/3.6728
            end)  bi_price
    from 
    ods.ods_bi_sell_price_sku 
    where 
    data_date = regexp_replace(to_date(date_sub(current_timestamp(),1)),'-','')--regexp_replace(to_date(current_timestamp()),'-','')
    and country = 'SA' 
    group by 1,2
    ),
    Break_price as ----jc的盈亏平衡价
    (
    select 
    rec_id sku_id,
    case when price1!=0 and price1 is not null then price1 else price2 end Break_price----盈亏平衡价
    from zybiro.tmp_faye_fashion_balance_price_lc30_new a 
    where data_date=regexp_replace(to_date(date_sub(current_timestamp(),1)),'-','')
    and site = 'jc'
    ),
    promote_price as  ----价格分默认和其他区域，暂取默认区域
    (
    select 
    sku_id ,
    goods_id,
    from_unixtime(promote_start_time,'yyyy-MM-dd') promote_start_time,
    from_unixtime(promote_end_time,'yyyy-MM-dd') promote_end_time,
    shop_price,  --商品吊牌
    promote_price  ---促销价
    from 
    jolly.who_sku_sell_price 
    where area_id = 1 
    ),
    conversion_funnel as 
    (
    select   --商品的 月销售数量、收入、吊牌、成本、曝光、点击、加购、收藏、 均价
    ws.goods_id
    ,sum(7d_sales_volume             )                       num_7day
    ,sum(7d_sales_amount             )                   revenue_7day
    ,sum(7d_sales_amount )/sum(7d_expo_pv)*1000         pv_value_7day
    ,sum(7d_cart_click )/sum(7d_goods_click)            click_to_cart_7day
    ,sum(30d_sales_volume)                              num_30day
    ,sum(30d_sales_amount             )                   revenue_30day
    ,sum(30d_sales_amount )/sum(30d_expo_pv)*1000           pv_value_30day
    ,sum(30d_goods_click )/sum(30d_expo_pv)         pv_to_click_30day
    from
    rpt.rpt_sum_goods_daily ws
    where
    ws.data_date = regexp_replace(to_date(date_sub(current_timestamp(),1)),'-','')
    and
    ws.site_id  in(400,600,700,601,900) 
    group by
    ws.goods_id
    ),
    ------该商品是否单一成本
    singe_inprice as 
    (
    select 
    goods_id,
    case when max(in_price)=min(in_price) then '单成本' else '多成本' end singe_inprice
    from 
    jolly.who_sku_relation 
    where status = 0
    group by goods_id
    ),
    singe_shop_price as  ----价格分默认和其他区域，暂取默认区域
    (
    select 
    ws.goods_id,
    case when max(shop_price)= min(shop_price) then '单吊牌价' else '多吊牌价' end singe_shop_price
    from 
    jolly.who_sku_sell_price  ws
    left join jolly.who_sku_relation  ws1 on ws.sku_id = ws1.rec_id
    where ws.area_id = 1 
    and ws1.status = 0
    group by goods_id 
    )

    select 
    *
    from  
        (
        ----选出商品合适的商品（1、单吊牌的商品能统一定价   2、成本价差异不超过1RMB的可以统一调整吊牌价 ） 
        select 
        case when singe_shop_price = '单吊牌价' and  singe_inprice = '单成本'  then 'goods定价' 
            when singe_shop_price = '单吊牌价' and  singe_inprice = '多成本'  then '供应商调价'
            else 'SKU定价' end '定价的维度'
        ,ws.goods_id
        -- ,ws.sku_id
        ,ws.BU
        ,ws.category_group
        ,ws.cate_level1_name
        ,ws.cate_level2_name
        ,ws.cate_level3_name
        ,ws.shop_price
        ,ws.in_price
        ,ws.price_now
        ,ws.price_now*0.82 'flash_price'
        ,(ws.price_now*0.82 - ws.in_price/7)/(ws.price_now*0.82)  'maolilv' 
        ,(ws.price_now*0.82 - Break_price)/(ws.price_now*0.82)  'jinglilv'  
        ,1-(ws.price_now*0.82/ws.shop_price) 'zhekou'   
        ,ws.price_now*0.82 - ws.price_now 'bijia'   
        ,500  'kucun'
        ,ws.first_on_sale_time
        ,ws.click_to_cart_7day
        ,ws.num_7day
        ,ws.revenue_7day
        ,ws.pv_value_7day
        ,ws.num_30day
        ,ws.revenue_30day
        ,Break_price
        from 
            (
            select 
            ws.*
            ,row_number() over (partition by ws.goods_id order by ws.in_price desc) in_price_rank
            from 
                (
                select 
                ws.rec_id  sku_id ,
                ws.goods_id,
                ws5.goods_name,
                case when ws5.category_group = 'beauty' then '美妆事业部'  else '家居母婴童事业部' end 'BU',
                ws5.cate_level1_name,
                ws5.cate_level2_name,
                ws5.cate_level3_name,
                ws4.shop_price , 
                ws.in_price   ,
                case when ws2.sku_id is not null  then ws2.bi_price 
                     when ws.promote_price= 0 then ws4.shop_price else ws.promote_price end  price_now,
                ws9.singe_inprice,
                ws10.singe_shop_price,
                ws5.provider_code,
                to_date(ws5.first_on_sale_time) first_on_sale_time,
                ws5.category_group,
                ws3.Break_price ,
                case when ws2.sku_id is null then '非智能' else '智能定价' end '是否智能定价',
                ws7.num_7day,
                ws7.revenue_7day,
                ws7.pv_value_7day,
                ws7.click_to_cart_7day,
                ws7.num_30day,
                ws7.revenue_30day,
                ws7.pv_value_30day,
                ws7.pv_to_click_30day
                from 
                jolly.who_sku_relation  ws
                left join on_sale  ws1 on ws.goods_id =ws1.goods_id 
                left join bi_price ws2 on ws.rec_id = ws2.sku_id
                left join Break_price ws3 on ws.rec_id = ws3.sku_id
                left join promote_price ws4 on ws.rec_id = ws4.sku_id
                left join dim.dim_goods ws5 on ws.goods_id = ws5.goods_id 
                left join conversion_funnel ws7 on ws.goods_id = ws7.goods_id 
                left join singe_inprice ws9 on ws.goods_id = ws9.goods_id
                left join singe_shop_price ws10 on ws.goods_id = ws10.goods_id
                where ws.status= 0
                and ws1.goods_id is not null 
                and ws5.category_group in ('婴童时尚',  '孕婴童用品') --,'家居','beauty') --in ('Home Linens' 'Event and Party Supplies','Home Supplies')  ---'Home Linens'  --Pet Supplies Home Decor  --ws5.cate_level1_name = 'Fragrances'  
                and ws5.supplier_genre  in (1,2)
                and ws7.num_30day > 0 
                ) ws 
            )ws 
            where  ( (singe_shop_price = '单吊牌价'  and in_price_rank = 1)  or singe_shop_price ='多吊牌价') 
        )ws 
    where ws.maolilv >= 0.45  and jinglilv >= -0.02 and zhekou >= 0.25
        and ((flash_price<=3.99 and num_7day >= 65) or   (flash_price<=5.99 and num_7day >= 6)  or flash_price>5.99)
    """
    return sqlmsg

def flash_week_beauty():
    sqlmsg = """
    with on_sale as  ---商品在架且能售
    (
    select 
    goods_id 
    from 
    dim.dim_goods_extend 
    where data_date = regexp_replace(to_date(date_sub(current_timestamp(),1)),'-','')
    and is_jc_on_sale = 1 
    and is_jc_sale =1 
    --and is_jc_enable = 1
    ),
    bi_price as  ---bi针对不同人群设定的最高价格 SA地区
    (
    select 
    goods_id ,
    sku_id,
    max(case when currency ='USD' then bi_price
            when currency ='QAR' then bi_price/3.64
            when currency ='SAR' then bi_price/3.75 
            when currency ='KWD' then bi_price/0.3036
            when currency ='AED' then bi_price/3.6728
            end)  bi_price
    from 
    ods.ods_bi_sell_price_sku 
    where 
    data_date = regexp_replace(to_date(date_sub(current_timestamp(),1)),'-','')--regexp_replace(to_date(current_timestamp()),'-','')
    and country = 'SA' 
    group by 1,2
    ),
    Break_price as ----jc的盈亏平衡价
    (
    select 
    rec_id sku_id,
    case when price1!=0 and price1 is not null then price1 else price2 end Break_price----盈亏平衡价
    from zybiro.tmp_faye_fashion_balance_price_lc30_new a 
    where data_date=regexp_replace(to_date(date_sub(current_timestamp(),1)),'-','')
    and site = 'jc'
    ),
    promote_price as  ----价格分默认和其他区域，暂取默认区域
    (
    select 
    sku_id ,
    goods_id,
    from_unixtime(promote_start_time,'yyyy-MM-dd') promote_start_time,
    from_unixtime(promote_end_time,'yyyy-MM-dd') promote_end_time,
    shop_price,  --商品吊牌
    promote_price  ---促销价
    from 
    jolly.who_sku_sell_price 
    where area_id = 1 
    ),
    conversion_funnel as 
    (
    select   --商品的 月销售数量、收入、吊牌、成本、曝光、点击、加购、收藏、 均价
    ws.goods_id
    ,sum(7d_sales_volume             )                       num_7day
    ,sum(7d_sales_amount             )                   revenue_7day
    ,sum(7d_sales_amount )/sum(7d_expo_pv)*1000         pv_value_7day
    ,sum(7d_cart_click )/sum(7d_goods_click)            click_to_cart_7day
    ,sum(30d_sales_volume)                              num_30day
    ,sum(30d_sales_amount             )                   revenue_30day
    ,sum(30d_sales_amount )/sum(30d_expo_pv)*1000           pv_value_30day
    ,sum(30d_goods_click )/sum(30d_expo_pv)         pv_to_click_30day
    from
    rpt.rpt_sum_goods_daily ws
    where
    ws.data_date = regexp_replace(to_date(date_sub(current_timestamp(),1)),'-','')
    and
    ws.site_id  in(400,600,700,601,900) 
    group by
    ws.goods_id
    ),
    ------该商品是否单一成本
    singe_inprice as 
    (
    select 
    goods_id,
    case when max(in_price)=min(in_price) then '单成本' else '多成本' end singe_inprice
    from 
    jolly.who_sku_relation 
    where status = 0
    group by goods_id
    ),
    singe_shop_price as  ----价格分默认和其他区域，暂取默认区域
    (
    select 
    ws.goods_id,
    case when max(shop_price)= min(shop_price) then '单吊牌价' else '多吊牌价' end singe_shop_price
    from 
    jolly.who_sku_sell_price  ws
    left join jolly.who_sku_relation  ws1 on ws.sku_id = ws1.rec_id
    where ws.area_id = 1 
    and ws1.status = 0
    group by goods_id 
    )

    select 
    *
    from  
        (
        ----选出商品合适的商品（1、单吊牌的商品能统一定价   2、成本价差异不超过1RMB的可以统一调整吊牌价 ） 
        select 
        case when singe_shop_price = '单吊牌价' and  singe_inprice = '单成本'  then 'goods定价' 
            when singe_shop_price = '单吊牌价' and  singe_inprice = '多成本'  then '供应商调价'
            else 'SKU定价' end '定价的维度'
        ,ws.goods_id
        -- ,ws.sku_id
        ,ws.BU
        ,ws.category_group
        ,ws.cate_level1_name
        ,ws.cate_level2_name
        ,ws.cate_level3_name
        ,ws.shop_price
        ,ws.in_price
        ,ws.price_now
        ,ws.price_now*0.82 'flash_price'
        ,(ws.price_now*0.82 - ws.in_price/7)/(ws.price_now*0.82)  'maolilv' 
        ,(ws.price_now*0.82 - Break_price)/(ws.price_now*0.82)  'jinglilv'  
        ,1-(ws.price_now*0.82/ws.shop_price) 'zhekou'   
        ,ws.price_now*0.82 - ws.price_now 'bijia'   
        ,500  'kucun'
        ,ws.first_on_sale_time
        ,ws.click_to_cart_7day
        ,ws.num_7day
        ,ws.revenue_7day
        ,ws.pv_value_7day
        ,ws.num_30day
        ,ws.revenue_30day
        ,Break_price
        from 
            (
            select 
            ws.*
            ,row_number() over (partition by ws.goods_id order by ws.in_price desc) in_price_rank
            from 
                (
                select 
                ws.rec_id  sku_id ,
                ws.goods_id,
                ws5.goods_name,
                case when ws5.category_group = 'beauty' then '美妆事业部'  else '家居母婴童事业部' end 'BU',
                ws5.cate_level1_name,
                ws5.cate_level2_name,
                ws5.cate_level3_name,
                ws4.shop_price , 
                ws.in_price   ,
                case when ws2.sku_id is not null  then ws2.bi_price 
                     when ws.promote_price= 0 then ws4.shop_price else ws.promote_price end  price_now,
                ws9.singe_inprice,
                ws10.singe_shop_price,
                ws5.provider_code,
                to_date(ws5.first_on_sale_time) first_on_sale_time,
                ws5.category_group,
                ws3.Break_price ,
                case when ws2.sku_id is null then '非智能' else '智能定价' end '是否智能定价',
                ws7.num_7day,
                ws7.revenue_7day,
                ws7.pv_value_7day,
                ws7.click_to_cart_7day,
                ws7.num_30day,
                ws7.revenue_30day,
                ws7.pv_value_30day,
                ws7.pv_to_click_30day
                from 
                jolly.who_sku_relation  ws
                left join on_sale  ws1 on ws.goods_id =ws1.goods_id 
                left join bi_price ws2 on ws.rec_id = ws2.sku_id
                left join Break_price ws3 on ws.rec_id = ws3.sku_id
                left join promote_price ws4 on ws.rec_id = ws4.sku_id
                left join dim.dim_goods ws5 on ws.goods_id = ws5.goods_id 
                left join conversion_funnel ws7 on ws.goods_id = ws7.goods_id 
                left join singe_inprice ws9 on ws.goods_id = ws9.goods_id
                left join singe_shop_price ws10 on ws.goods_id = ws10.goods_id
                where ws.status= 0
                and ws1.goods_id is not null 
                and ws5.category_group in ('beauty') --,'家居','beauty') --in ('Home Linens' 'Event and Party Supplies','Home Supplies')  ---'Home Linens'  --Pet Supplies Home Decor  --ws5.cate_level1_name = 'Fragrances'  
                and ws5.supplier_genre  in (1,2)
                and ws7.num_30day > 0 
                ) ws 
            )ws 
            where  ( (singe_shop_price = '单吊牌价'  and in_price_rank = 1)  or singe_shop_price ='多吊牌价') 
        )ws 
    where ws.maolilv >= 0.45  and jinglilv >= -0.02 and zhekou >= 0.25
        and ((flash_price<=3.99 and num_7day >= 65) or   (flash_price<=5.99 and num_7day >= 6)  or flash_price>5.99)
    """
    return sqlmsg


def stock_tianjian():
    sqlmsg = """
    select
        a0.*
    from(
        select
            cat.category_group,
            case when dg.cate_level1_name="Beauty" then dg.cate_level2_name else dg.cate_level1_name end as cate_level1_name,
            substr(dau.data_date,1,6) as data_date,
            dau.goods_id,
            sum(dau.expo_pv) as expo_pv,
            sum(dau.expo_uv) as expo_uv,
            sum(dau.goods_click_uv) as goods_click_uv,
            sum(dau.bill_uv) as bill_uv,
            sum(dau.bill_uv)/sum(dau.goods_click_uv) as click_bill,
            sum(dau.sales_uv) as sales_uv,
            sum(dau.sales_uv)/sum(dau.goods_click_uv) as click_sale
        from rpt.rpt_sum_goods_daily as dau
        left join dim.dim_goods as dg
        on dau.goods_id = dg.goods_id
        left join dim.dim_goods_category_group_new as cat
        on dg.cate_level1_name = cat.cate_level1_name
        left join zybiro.bi_longjy_category_group_new as cg
        on dg.cate_level1_name = cg.cate_level1_name
        where dau.data_date >= '20200301'
        and dau.data_date <= from_timestamp(date_sub(now(),1),'yyyyMMdd')
        and dau.site_id in (400,600,700,601,900)
        and cg.department = '三部'
        group by 1,2,3,4
    ) as a0
    where a0.expo_pv > 0
    """
    return sqlmsg


if __name__ == '__main__':
    # smg = tmp_sql()
    # print(smg)
    # # print(goods_no_pop_sql)
    print(new_goods_no_pop_sql('家居'))
    # sqlmsg = kids_week_month_sql('20191109', '20191116')
    # print(sqlmsg)
    # ss = rpts_daily_2018('20180116', '20180131')
    # print(ss)