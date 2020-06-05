import pandas as pd

def get_goods_df(goods_path):
    return pd.read_excel(goods_path)

def get_ratio_df(ratio_path):
    return pd.read_excel(ratio_path)

def create_goods_table(cat_df, rat_df):
    new_df = pd.DataFrame(columns=['goods_id', 'rec_id', 'in_price', 'in_price_usd', 'min', 'max', 'ratio'])
    for gi in cat_df.index:
        for ri in rat_df[rat_df['catId'] == cat_df.loc[gi, 'cate_level5_id']].index:
            if cat_df.loc[gi, 'in_price'] > rat_df.loc[ri, 'min'] and cat_df.loc[gi, 'in_price'] <= rat_df.loc[ri, 'max']:
                new_df = new_df.append({'goods_id': cat_df.loc[gi, 'goods_id'], 
                                        'rec_id': cat_df.loc[gi, 'rec_id'],
                                        'in_price': cat_df.loc[gi, 'in_price'], 
                                        'in_price_usd': cat_df.loc[gi, 'in_price_usd'],
                                        'min': rat_df.loc[ri, 'min'], 
                                        'max': rat_df.loc[ri, 'max'], 
                                        'ratio': rat_df.loc[ri, 'ratio']}, ignore_index=True)
    return new_df

def table_sive(df, sive_path):
    df.to_excel(sive_path)


if __name__ == '__main__':
    cat_df = get_goods_df(r'C:\Users\WIN7\Desktop\下载文件\商品售价导入\套装\套装20200330v2.xlsx')
    rat_df = get_ratio_df(r'C:\Users\WIN7\Desktop\下载文件\商品售价导入\importPriceRatio-6.xls')
    df = create_goods_table(cat_df, rat_df)
    table_sive(df, r'C:\Users\WIN7\Desktop\下载文件\商品售价导入\套装\goods_ratio_套装.xlsx')

    # cat_df = get_goods_df(r'C:\Users\WIN7\Desktop\下载文件\商品售价导入\kids\goods_tmp_kids_500000_1000000_lm.xlsx')
    # rat_df = get_ratio_df(r'C:\Users\WIN7\Desktop\下载文件\商品售价导入\importPriceRatio-6.xls')
    # df = create_goods_table(cat_df, rat_df)
    # table_sive(df, r'C:\Users\WIN7\Desktop\下载文件\商品售价导入\kids\ratio_goods\goods_ratio_婴童时尚_500000_1000000.xlsx')


    # cat_df = get_goods_df(r'C:\Users\WIN7\Desktop\下载文件\商品售价导入\kids\goods_tmp_kids_1000000_1500000_lm.xlsx')
    # rat_df = get_ratio_df(r'C:\Users\WIN7\Desktop\下载文件\商品售价导入\importPriceRatio-6.xls')
    # df = create_goods_table(cat_df, rat_df)
    # table_sive(df, r'C:\Users\WIN7\Desktop\下载文件\商品售价导入\kids\ratio_goods\goods_ratio_婴童时尚_1000000_1500000.xlsx')


    # cat_df = get_goods_df(r'C:\Users\WIN7\Desktop\下载文件\商品售价导入\kids\goods_tmp_kids_1500000_2000000_lm.xlsx')
    # rat_df = get_ratio_df(r'C:\Users\WIN7\Desktop\下载文件\商品售价导入\importPriceRatio-6.xls')
    # df = create_goods_table(cat_df, rat_df)
    # table_sive(df, r'C:\Users\WIN7\Desktop\下载文件\商品售价导入\kids\ratio_goods\goods_ratio_婴童时尚_1500000_2000000.xlsx')


    # cat_df = get_goods_df(r'C:\Users\WIN7\Desktop\下载文件\商品售价导入\kids\goods_tmp_kids_2000000_2500000_lm.xlsx')
    # rat_df = get_ratio_df(r'C:\Users\WIN7\Desktop\下载文件\商品售价导入\importPriceRatio-6.xls')
    # df = create_goods_table(cat_df, rat_df)
    # table_sive(df, r'C:\Users\WIN7\Desktop\下载文件\商品售价导入\kids\ratio_goods\goods_ratio_婴童时尚_2000000_2500000.xlsx')
    