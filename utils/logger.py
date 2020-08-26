"""
利用logging模块封装日志器

fmt格式说明：
%(name)s Logger的名字
%(levelno)s 数字形式的日志级别
%(levelname)s 文本形式的日志级别
%(pathname)s 调用日志输出函数的模块的完整路径名，可能没有
%(filename)s 调用日志输出函数的模块的文件名
%(module)s 调用日志输出函数的模块名
%(funcName)s 调用日志输出函数的函数名
%(lineno)d 调用日志输出函数的语句所在的代码行
%(created)f 当前时间，用UNIX标准的表示时间的浮点数表示
%(relativeCreated)d 输出日志信息时的，自Logger创建以来的毫秒数
%(asctime)s 字符串形式的当前时间，默认格式是 “2020-06-22 16:49:45,896”，逗号后面的是毫秒
%(thread)d 线程ID，可能没有
%(threadName)s 线程名，可能没有
%(process)d 进程ID，可能没有
%(message)s 用户输出的消息

datefmt格式说明：
%y 两位数的年份表示（00-99）
%Y 四位数的年份表示（000-9999）
%m 月份（01-12）
%d 月内中的一天（0-31）
%H 24小时制小时数（0-23）
%I 12小时制小时数（01-12）
%M 分钟数（00=59）
%S 秒（00-59）
%a 本地简化星期名称
%A 本地完整星期名称
%b 本地简化的月份名称
%B 本地完整的月份名称
%c 本地相应的日期表示和时间表示
%j 年内的一天（001-366）
%p 本地A.M.或P.M.的等价符
%U 一年中的星期数（00-53）星期天为星期的开始
%w 星期（0-6），星期天为星期的开始
%W 一年中的星期数（00-53）星期一为星期的开始
%x 本地相应的日期表示
%X 本地相应的时间表示
%Z 当前时区的名称
%% %号本身
"""

import os
import time
import logging
import services
from config import dk_plan, ding_interval
from utils import common_function as cf


class Logger(object):
    """
    日志器
    """

    def __init__(self, level='warning', fmt=None, date_fmt=None, log_path='log', log_name=None):
        """
        初始配置
        :param level:(type=str) 日志等级，默认WARNING等级
        :param fmt:(type=str) 日志格式，有默认格式
        :param date_fmt:(type=str) 时间格式，有默认格式
        :param log_path:(type=str) 存放日志的路径，默认当前路径下的log文件夹
        :param log_name:(type=str) 日志文件名称，默认以当天时间命名
        """

        # Redis
        self.redis = services.redis['127_3']

        # 获取一个logger对象
        self.__logger = logging.getLogger()

        # 设置format对象
        if fmt is None:
            fmt = '%(asctime)s %(filename)s [line:%(lineno)d] %(levelname)s: %(message)s'
        if date_fmt is None:
            date_fmt = '%Y-%m-%d %H:%M:%S'
        self.__file_formatter = logging.Formatter(fmt='\n\n\n' + fmt, datefmt=date_fmt)
        self.__console_formatter = logging.Formatter(fmt=fmt, datefmt=date_fmt)

        # 设置文件日志模式
        if log_name is None:
            log_name = time.strftime('%Y%m%d')
        self.__log_path = log_path
        self.__logger.addHandler(self.__get_file_handler(log_name))

        # 设置终端日志模式
        self.__logger.addHandler(self.__get_console_handler())

        # 设置日志等级
        self.__logger.setLevel(getattr(logging, level.upper()))

    def __get_file_handler(self, filename):
        """
        返回一个文件日志handler
        :param filename:(type=str) 日志文件名
        :return file_handler:(type=FileHandler) 文件日志模式对象
        """

        # 构建日志路径，如文件夹不存在，则创建
        if not os.path.isdir(self.__log_path):
            try:
                os.makedirs(self.__log_path)
            except FileExistsError:
                pass
        log_path = os.path.join(self.__log_path, filename)

        # 获取一个文件日志handler
        file_handler = logging.FileHandler(filename=log_path, encoding='utf8')

        # 设置日志格式
        file_handler.setFormatter(self.__file_formatter)

        # 返回
        return file_handler

    def __get_console_handler(self):
        """
        返回一个终端日志handler
        :return console_handler:(type=StreamHandler) 终端日志模式对象
        """

        # 获取一个输出到终端日志handler
        console_handler = logging.StreamHandler()

        # 设置日志格式
        console_handler.setFormatter(self.__console_formatter)

        # 返回
        return console_handler

    def __ding_exception(self, msg, e, key='', group=dk_plan, *args, **kwargs):
        """
        记录日志，并发送钉钉消息
        :param msg:(type=str) 报错消息
        :param msg:(type=Exception) 错误对象
        :param key:(type=str) 报错的额外标识，比如业务名称，默认空字符串
        :param group:(type=str) 要发送消息的群组，默认发去计划任务群组
        :param args:(type=tuple) 其余的执行日志器原生exception方法的参数
        :param kwargs:(type=dict) 其余的执行日志器原生exception方法的参数
        """

        # 执行日志器原生exception方法
        self.__logger.exception(msg, *args, **kwargs)

        # 尝试发送钉钉消息
        try:

            # 1.计算特征值
            fp_key = cf.calculate_fp([key, str(type(e))])

            # 2.根据特征值，获取Redis信息
            redis_result = self.redis.get(fp_key)

            # 3.1 Redis信息已过期（不存在），发送钉钉
            if redis_result is None:
                ding_result = cf.send_ding(msg, group)

                # 3.2 重设Redis消息
                if ding_result:
                    self.redis.set(fp_key, time.strftime('%Y-%m-%d %H:%M:%S'), ex=ding_interval)

        # 失败则不再发送，记录日志
        except Exception as e:
            self.__logger.exception('钉钉消息发送失败！%s' % e)

    @property
    def logger(self):
        """
        返回日志器对象
        :return log:(type=RootLogger) 日志器对象
        """

        log = self.__logger
        log.ding_exception = self.__ding_exception
        return log
