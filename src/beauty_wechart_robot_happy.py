import requests

# robot_url = 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=19ac6b7c-cc47-485f-bd7b-7bfc821da7c0' 
robot_url = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=45b58faa-400c-4b97-a4cb-75dbeff0dda6"  # 美妆机器人



def wechat_robot_text(msg, guy_list=None):
    # print(guy_list)
    headers = {"Content-Type": "text/plain"}
    s = msg
    data = {
        "msgtype": "text",
        "text": {
            "content": s,
            "mentioned_mobile_list": guy_list
        }
    }
    r = requests.post(
        url=robot_url,
        headers=headers, json=data
    )
    print(r.text)


if __name__ == '__main__':
    wechat_robot_text(msg="How are you", guy_list=['15858472365', '18503072970', '15757120600', '13666618978'])