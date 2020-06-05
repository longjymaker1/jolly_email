from src import wechat_robot, save_datatable_png
import pandas as pd


def send_on_goods_num():
    """发送在架商品数数据"""
    png_path = save_datatable_png.save_on_sale_goods_png()
    wechat_robot.wechat_robot_text("在架商品数")
    wechat_robot.wechat_robot_image(png_path)


def send_new_goods_num():
    """发送上新商品数数据"""
    png_path = save_datatable_png.save_new_goods_num_png()
    wechat_robot.wechat_robot_text("上新商品数")
    wechat_robot.wechat_robot_image(png_path)


def send_gmv_income(datadf):
    """发送GMV达成数据"""
    png_path = save_datatable_png.save_gmv_income_png(datadf)
    wechat_robot.wechat_robot_text("达成")
    wechat_robot.wechat_robot_image(png_path)


def send_bu_data(datadf):
    """发送bu当月数据"""
    # file_path, yoy_file_path = save_datatable_png.bu_data_png_save(datadf)
    datadf['data_date'] = pd.to_datetime(datadf['data_date'], format="%Y%m%d")
    datadf = datadf.fillna(0)
    this_year_gmv_self_df = save_datatable_png.this_year_gmv_self(datadf)
    this_year_gmv_all_df = save_datatable_png.this_year_gmv_pop(datadf)
    this_year_margin_df = save_datatable_png.this_year_margin(datadf)
    this_year_dau_df = save_datatable_png.this_year_dau(datadf)
    this_year_dau_ratio_df = save_datatable_png.this_year_dau_ratio(datadf)

    last_year_gmv_self_df = save_datatable_png.last_year_gmv_self(datadf)
    last_year_gmv_all_df = save_datatable_png.last_year_gmv_pop(datadf)
    last_year_margin_df = save_datatable_png.last_year_margin(datadf)

    wechat_robot.wechat_robot_image(this_year_gmv_self_df)
    wechat_robot.wechat_robot_image(this_year_gmv_all_df)
    wechat_robot.wechat_robot_image(this_year_margin_df)
    wechat_robot.wechat_robot_image(this_year_dau_df)
    wechat_robot.wechat_robot_image(this_year_dau_ratio_df)
    
    wechat_robot.wechat_robot_image(last_year_gmv_self_df)
    wechat_robot.wechat_robot_image(last_year_gmv_all_df)
    wechat_robot.wechat_robot_image(last_year_margin_df)


if __name__ == '__main__':
    try:
        send_on_goods_num()
    except Exception as identifier:
        print(identifier)
    
    try:
        send_new_goods_num()
    except Exception as identifier:
        print(identifier)

    try:
        datadf = save_datatable_png.get_gmv_data()
        send_gmv_income(datadf)
        send_bu_data(datadf)
    except Exception as identifier:
        print(identifier)