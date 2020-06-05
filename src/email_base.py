#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/7/25 15:50
# @Author  : Long
# @Site    : 
# @File    : email_base.py
# @Software: PyCharm
"""
发送邮件
"""
import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.header import Header

main_host = 'smtp.jollycorp.com'
main_user = 'long.long@jollycorp.com'
# main_pwd = 'Longjianyu6789'
main_pwd = 'o7zTbCRXEWeUhduW'


class emailSend:
    def __init__(self, host=main_host, user=main_user, pwd=main_pwd, **kwargs):
        """
        发送邮件
        :param host: 发送邮件服务器host
        :param user: 发送人
        :param pwd: 发送人密码
        :param kwargs: users, 接收邮件人, list; title, 邮件标题, str; file_path, 发送邮件的文件路径(包含文件名)
        """
        self.host = host
        self.user = user
        self.pwd = pwd
        self.kwargs = kwargs

    def sender(self, content=None):
        """发送邮件, 并返回结果"""

        msg = MIMEMultipart('alternative')
        msg['From'] = self.user
        msg['Subject'] = self.kwargs['title']
        msg['To'] = ",".join(self.kwargs['users'])
        msg.attach(MIMEText(content, 'html', 'utf-8'))
        if 'file_path' in self.kwargs:
            print(self.kwargs)
            xlspart0 = MIMEApplication(open(self.kwargs['file_path'], 'rb').read())
            xlspart0.add_header('Content-Disposition', 'attachment', filename=self.kwargs['file_path'].split('/')[-1])
            msg.attach(xlspart0)  # 把附件添加到邮件中
        try:
            s = smtplib.SMTP()
            s = smtplib.SMTP_SSL(self.host, 465)
            s.connect(self.host)
            # login
            s.login(self.user, self.pwd)
            # send mail
            s.sendmail(self.user, self.kwargs['users'], msg.as_string())
            s.close()
            print('success')
        except Exception as e:
            print('Exception: ', e)