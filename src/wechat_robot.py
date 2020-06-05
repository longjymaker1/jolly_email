import requests
from pandas.plotting import table
import matplotlib
import hashlib
import base64
import os

matplotlib.rcParams['font.sans-serif'] = ['SimHei']
matplotlib.rcParams['font.family'] = 'sans-serif'

matplotlib.rcParams['axes.unicode_minus'] = False

robot_url = 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=ee2d1a2c-5b3c-4460-9d6f-a6dac8fcf776'  # 正式机器人
# robot_url = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=19ac6b7c-cc47-485f-bd7b-7bfc821da7c0"  # 测试机器人
path_imgs = r"F:\pro\JollychicEmail\excel_file\imgs"


def wechat_robot_text(msg):
    headers = {"Content-Type": "text/plain"}
    s = msg
    data = {
        "msgtype": "text",
        "text": {
            "content": s,
        }
    }
    r = requests.post(
        url=robot_url,
        headers=headers, json=data
    )
    print(r.text)


def wechat_robot_image(img_path):
    file = open(img_path, "rb")
    md = hashlib.md5()
    md.update(file.read())
    res1 = md.hexdigest()

    # 图片base64码
    with open(img_path,"rb") as f:
        base64_data = base64.b64encode(f.read())


    headers = {"Content-Type": "text/plain"}
    s = img_path
    data = {
        "msgtype": "image",
        "image": {
            "base64": base64_data,
            "md5": res1
        }
    }
    r = requests.post(
        url=robot_url,
        headers=headers, json=data
    )
    print(r.text)


if __name__ == '__main__':
    # wechat_robot_text('sdfsdfdfff')
    img_name = os.path.join(path_imgs, "new_goods_20200323.png")
    # wechat_robot_image(img_name)
