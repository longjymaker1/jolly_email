import pandas as pd
import os


def get_excel_data(file_source: str):
    """获取excel数据"""
    return pd.read_excel(file_source)


def cut_df(df):
    """根据序号切分数据"""
    rn_lst = list(set(df['rn']))
    for rn in rn_lst:
        yield df[df['rn'] == rn]


def save_df(rn_df, cate_name):
    """将切分后的dataframe写入新的excel中，并将类目+_rn.xlsx 作为新的文件名"""
    rn_num = list(set(rn_df['rn']))[0]
    dirname = r"C:\Users\WIN7\Desktop\下载文件\商品售价导入\{cate_name}".format(cate_name=cate_name)
    file_name = r"{cate_name}_{rn}.xlsx".format(cate_name=cate_name, rn=rn_num)
    file_path = os.path.join(dirname, file_name)
    rn_df = rn_df.drop('rn', axis=1)
    rn_df.to_excel(file_path, index=False)


def hanld(cate_name, base_file_path):
    """操作权柄"""
    data = get_excel_data(base_file_path)
    for i in cut_df(data):
        save_df(i, cate_name)


if __name__ == "__main__":
    print(hanld.__name__)  # 获取函数名
    print(hanld.__doc__)  # 获取注释
