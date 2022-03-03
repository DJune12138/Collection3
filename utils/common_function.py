"""
非业务公共函数
"""

import sys
import time
import hashlib
import requests
import json
import datetime
import base64
import subprocess
import pytz
import services  # 该模块在加载服务前就已经被导入，只能导入总模块，否则所有服务都会是加载前的None
from config import format_date, format_date_n, format_datetime_n


def print_log(msg):
    """
    打印指定格式日志信息
    :param msg:(type=str) 日志信息
    """

    pid = '(%s)' % services.launch['pid'] if services.launch is not None else ''
    print('%s%s：%s' % (time.strftime(format_datetime_n), pid, msg))
    sys.stdout.flush()


def calculate_fp(parameters, algorithms='sha1'):
    """
    计算特征值
    :param parameters:(type=list,str,bytes) 参数列表，为list类型时则依次添加特征数据算特征值，元素均为str或bytes类型
    :param algorithms:(type=str) 算法，默认sha1
    :return fp:(type=str) 特征值计算结果
    """

    # 校验算法是否为官方支持算法，并生成算法对象
    available_list = hashlib.algorithms_available  # 官方支持算法
    if algorithms not in available_list:
        raise ValueError('algorithms（算法）只能为%s中的一种！' % '、'.join(available_list))
    hash_object = getattr(hashlib, algorithms)()

    # 校验parameters参数类型是否正确
    if isinstance(parameters, str) or isinstance(parameters, bytes):
        parameters = [parameters]
    elif isinstance(parameters, list):
        pass
    else:
        raise TypeError('parameters应为list、str或bytes类型！')

    # 校验特征数据类型并计算特征值，返回特征值
    for parameter in parameters:
        if isinstance(parameter, str):
            hash_object.update(parameter.encode('utf8'))  # 计算的对象必须是字节类型
        elif isinstance(parameter, bytes):
            hash_object.update(parameter)
        else:
            TypeError('parameters为list类型时，元素应为str或bytes类型！')
    fp = hash_object.hexdigest()
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
    :param kwargs:(type=dict) 其余的关键字参数，用于接收请求头与请求体
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
                files = kwargs.get('files')
                if files is None:
                    response = requests.post(url, timeout=timeout, headers=kwargs['headers'], data=kwargs['data'])
                else:
                    response = requests.post(url, timeout=timeout, headers=kwargs['headers'], data=kwargs['data'],
                                             files=files)
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
    :param kwargs:(type=dict) 其余的关键字参数，用于接收请求方式、请求头、请求体等，传参给获取请求数据的函数使用
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


def change_time_format(time_str, before=format_date, after=format_date_n, interval=0):
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


def shell_run(shell, cwd=None, timeout=None, check=True, pr_std=True, **kwargs):
    """
    执行shell命令，关于subprocess参考：https://www.jianshu.com/p/592202895978
    :param shell:(type=str) 要执行的shell命令
    :param cwd:(type=str) 执行命令前要切换的工作目录，默认不切换
    :param timeout:(type=int) 超时（单位：秒），超出时间结束执行并抛异常（对象属性参考上述网址），默认不设置
    :param check:(type=bool) 如执行完毕后状态码为非0（有报错），则抛异常，默认True
    :param pr_std:(type=bool) 执行完成后是否把标准错误与标准输出打印至控制台，默认True
    :param kwargs:(type=dict) 防止传入额外关键字参数报错
    :return result:(type=CompletedProcess) 执行结果的对象（对象属性参考上述网址）
    """

    result = subprocess.run(shell, shell=True, capture_output=True, cwd=cwd, timeout=timeout, check=check)
    if pr_std:
        try:
            stdout = result.stdout.decode()
        except UnicodeDecodeError:
            stdout = result.stdout.decode('gbk')
        print('stdout：%s' % stdout)
        try:
            stderr = result.stderr.decode()
        except UnicodeDecodeError:
            stderr = result.stderr.decode('gbk')
        print('stderr：%s' % stderr)
    return result


def timestamp_format(timestamp, format_=format_datetime_n, time_type='lt'):
    """
    时间戳格式化
    :param timestamp:(type=int,float) 时间戳，注意单位是秒，毫秒请自行除以1000转换成秒
    :param format_:(type=str) 格式化样式，默认“%Y-%m-%d %H:%M:%S”
    :param time_type:(type=str) 要转换时间的时区，lt为本地时间，gt为格林威治时间（可用于把秒数格式化），默认本地时间
    :return format_str:(type=str) 格式化时间
    """

    time_type = time_type.lower()
    if time_type == 'lt':
        format_str = time.strftime(format_, time.localtime(timestamp))
    elif time_type == 'gt':
        format_str = time.strftime(format_, time.gmtime(timestamp))
    else:
        raise ValueError('time_type只能为“lt”（本地时间）或“gt”（格林威治时间）！')
    return format_str


def change_timezone(change, t_timezone, l_timezone='Asia/Shanghai', time_format=format_datetime_n, use_obj=False):
    """
    把一个时区的时间转去另外一个时区的时间
    :param change:(type=str,datetime) 要转换的时间字符串，如use_obj=True则为datetime对象
    :param t_timezone:(type=str) 目标时区，具体写法参照pytz.timezone说明，下同
    :param l_timezone:(type=str) 本地时区，默认东八区
    :param time_format:(type=str) 要转换的时间字符串格式，默认“%Y-%m-%d %H:%M:%S”，只在use_obj=False有作用
    :param use_obj:(type=bool) 输入和输出是否使用datetime对象，默认False不使用对象则为字符串
    :return target_tz:(type=str,datetime) 转换的结果，默认根据time_format格式返回字符串，如use_obj=True则为datetime对象
    """

    # 时区
    local_tz = pytz.timezone(l_timezone)  # 本地
    target_tz = pytz.timezone(t_timezone)  # 目标

    # 实例化本地时区
    dt_obj = datetime.datetime.strptime(change, time_format) if not use_obj else change
    local_obj = local_tz.localize(dt_obj)

    # 转换成目标时区
    dt_obj = local_obj.astimezone(tz=target_tz)
    target_tz = dt_obj.strftime(time_format) if not use_obj else dt_obj
    return target_tz
