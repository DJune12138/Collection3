"""
非业务公共函数
"""

import os
import sys
import time
import hashlib
import requests
import json
import datetime
import base64
import socket
from config import ding_token, factory_config, factory_code


def __get_local_ip():
    """
    获取本机外网IP地址
    :return:(type=str) 本机外网IP地址
    """

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
    finally:
        s.close()
    return ip


def print_log(msg):
    """
    打印指定格式日志信息
    :param msg:(type=str) 日志信息
    """

    print('%s(%s)：%s' % (time.strftime('%Y-%m-%d %H:%M:%S'), __pid, msg))
    sys.stdout.flush()


def calculate_fp(parameters):
    """
    计算特征值
    :param parameters:(type=list) 参数列表，元素均为str或bytes类型
    :return:
    """

    s1 = hashlib.sha1()
    for parameter in parameters:
        if isinstance(parameter, str):
            s1.update(parameter.encode('utf8'))  # sha1计算的对象必须是字节类型
        elif isinstance(parameter, bytes):
            s1.update(parameter)
        else:
            TypeError('parameters元素均为str或bytes类型！')
    fp = s1.hexdigest()
    return fp


def request_get_response(url, method='get', interval=0, retry_interval=1, timeout=15, retry=3, **kwargs):
    """
    获取响应数据，重连失败抛IOError异常。
    :param url:(type=str) 请求地址
    :param method:(type=str) 请求方式，get或post，默认get
    :param interval:(type=int) 请求间隔时间（秒），默认0
    :param retry_interval:(type=int) 重连请求间隔时间（秒），默认1
    :param timeout:(type=int) 超时时间（秒），默认30
    :param retry:(type=int) 重连次数，默认3
    :param kwargs:(type=dict) 其余的命名参数，用于接收请求头与请求体
    :return response:(type=Response) 响应数据，如果多次尝试请求仍然失败，抛异常。
    """

    # 创建默认请求头与请求体
    kwargs.setdefault('headers', {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/83.0.4103.116 Safari/537.36 '
    })
    kwargs.setdefault('data', dict())

    # 断线重连，重连次数达上限后抛异常
    for i in range(retry):
        time.sleep(interval)
        try:
            if method.lower() == 'get':
                response = requests.get(url, timeout=timeout, headers=kwargs['headers'])
            elif method.lower() == 'post':
                response = requests.post(url, timeout=timeout, headers=kwargs['headers'], data=kwargs['data'])
            else:
                raise ValueError('method只能为"get"或"post"！')
        except requests.exceptions.RequestException as e:
            if i == retry - 1:
                raise IOError('请求失败，请排查！（报错信息：%s）；（URL：%s）；（请求头：%s）；（请求体：%s）'
                              % (e, url, kwargs['headers'], kwargs['data']))
            else:
                time.sleep(retry_interval)
        else:
            return response


def json_loads(json_str):
    """
    解析json字符串，主要封装处理json解析出错的问题，出错统一抛ValueError异常
    :param json_str:(type=str) json字符串
    :return json_data:(type=dict,list) 解析后的json数据
    """

    try:
        json_data = json.loads(json_str)
    except TypeError as e:
        raise ValueError('传参类型错误！（报错信息：%s）；（传参str：%s）' % (e, str(json_str)))
    except json.decoder.JSONDecodeError as e:
        raise ValueError('json格式错误！（报错信息：%s）；（传参str：%s）' % (e, json_str))
    else:
        return json_data


def repetition_json(url, interval_=1, retry_=3, **kwargs):
    """
    偶尔出现网络请求得到的响应数据不是标准json的问题，可通过再次请求解决，请求成功则返回解析后的json数据
    :param url:(type=str) 请求地址
    :param interval_:(type=int) 重试间隔时间（秒），默认1
    :param retry_:(type=int) 重试次数，默认3
    :param kwargs:(type=dict) 其余的命名参数，用于接收请求方式、请求头、请求体等，传参给获取请求数据的函数使用
    :return json_data:(type=dict,list) 解析后的json数据
    """

    for i in range(retry_):
        response = request_get_response(url, **kwargs)
        try:
            json_data = json_loads(response.content.decode())
        except ValueError as e:
            if i == retry_ - 1:
                raise e
            else:
                time.sleep(interval_)
        else:
            return json_data


def datetime_timedelta(td_type, td_count, date_time=None, format_=None):
    """
    把datetime对象根据对应时间间隔获取全新时间
    :param td_type:(type=str) 间隔类型，days、hours等
    :param td_count:(type=int,float) 间隔数量
    :param date_time:(type=datetime) 准备转换的时间，默认当前时间
    :param format_:(type=str) 如果有时间格式，则返回对应时间格式的字符串，否则返回转换后的datetime对象，默认返回datetime对象
    :return new_datetime:(type=str,datetime) 转换后的全新时间
    """

    if date_time is None:
        date_time = datetime.datetime.now()
    kwargs = {td_type: td_count}
    new_datetime = date_time + datetime.timedelta(**kwargs)
    if format_:
        new_datetime = new_datetime.strftime(format_)
    return new_datetime


def change_time_format(time_str, before='%Y%m%d', after='%Y-%m-%d', interval=0):
    """
    把时间字符串从一种格式转换去另一种格式，也可以根据间隔获取全新时间
    :param time_str:(type=str) 要转换的时间字符串
    :param before:(type=str) 转换前的格式，默认“%Y%m%d”
    :param after:(type=str) 转换后的格式，默认“%Y-%m-%d”
    :param interval:(type=int,float) 差距时间（秒），如传入则根据差距时间得到全新时间，默认0
    :return new_str:(type=str) 转换后的时间字符串
    """

    if not interval:
        new_str = time.strftime(after, time.strptime(time_str, before))
    else:
        new_str = time.strftime(after, time.localtime(time.mktime(time.strptime(time_str, before)) + interval))
    return new_str


def base64_change(content, encode=True, charset='utf8', re_str=True):
    """
    进行base64的编码解码
    :param content:(type=str,bytes) 要编码或解码的内容
    :param encode:(type=bool) 编码还是解码，True为编码，False为解码，默认编码
    :param charset:(type=str) 字符集，默认utf8
    :param re_str:(type=bool) 是否返回字符串，默认True
    :return new_content:(type=str,bytes) 编码或解码后的内容
    """

    # 兼容None
    if content is None:
        content = ''

    # 编码
    if encode:
        if isinstance(content, str):
            content = content.encode(charset)
        new_content = base64.b64encode(content)

    # 解码
    else:
        new_content = base64.b64decode(content)

    # 根据re_str返回字符串或二进制
    if re_str:
        new_content = new_content.decode()
    return new_content


def send_ding(msg, group):
    """
    发送钉钉消息
    :param msg:(type=str) 要发送的钉钉信息内容
    :param group:(type=str) 要发送的钉钉群组
    :return result:(type=bool) 发送结果，成功为True，失败为False
    """

    # 校验group
    group_list = ding_token.keys()
    if group not in group_list:
        raise ValueError('group只能为%s其中一个！' % '、'.join(group_list))

    # 钉钉消息内容
    log_path = factory_config[factory_code]['logger_config.log_path']
    log_path = log_path if log_path is not None else os.path.join(os.getcwd(), 'log')
    ding_msg = '任务出错了！详情请查看报错日志！\n执行任务的主机IP：%s\n报错日志路径：%s\n报错信息：%s' % (__ip, log_path, msg)

    # 发送钉钉消息
    url = 'https://oapi.dingtalk.com/robot/send?access_token=%s' % ding_token[group]
    data = {
        'msgtype': 'text',
        'text': {
            'content': ding_msg
        }
    }
    headers = {
        'Content-Type': 'application/json;charset=utf-8'
    }
    response = repetition_json(url, method='post', headers=headers, data=json.dumps(data))
    if response['errcode'] == 0:
        print_log('钉钉消息发送成功！')
        result = True
    else:
        print_log('钉钉消息发送失败！')
        result = False

    # 返回发送结果
    return result


# 以下为其他函数用到的公共参数
__ip = None  # 本机ip
if __ip is None:
    __ip = __get_local_ip()
__pid = None  # 当前进程id
if __pid is None:
    __pid = os.getpid()
