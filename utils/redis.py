"""
Redis数据库连接池
1.目前暂时都是免密登录
2.其他说明与MySQL一致，可参考MySQL说明
"""

import redis
from threading import Lock
from utils import common_function as cf
from config import account_name


class RedisError(Exception):
    """
    Redis数据库连接池的异常类基类
    """

    def __init__(self, info=None):
        """
        初始配置
        :param info:(type=str) 报错提示信息，默认无
        """

        self.info = info


class RepetitiveConnect(RedisError):
    """
    重复连接
    """

    def __str__(self):
        """
        异常描述信息
        :return info:(type=str) 异常描述
        """

        info = '相同配置的Redis连接已创建！请勿重复创建连接，造成资源浪费！（Tips：如不同业务用同配置的连接，可在%s模块的Redis配置里添加）' % account_name
        return info


class ConnectFailed(RedisError):
    """
    连接失败
    """

    def __init__(self, info):
        """
        初始配置
        :param info:(type=str) 原生报错提示信息
        """

        self.info = info

    def __str__(self):
        """
        异常描述信息
        :return info:(type=str) 异常描述
        """

        info = 'Redis连接失败，请确认配置连接信息和数据库权限是否正确！%s' % self.info
        return info


class Redis(object):
    """
    Redis数据库连接池
    """

    # 去重容器，存储已经成功连接的配置信息的特征值
    # 配置信息为host、port、db
    __filter_container = set()

    # 互斥锁，防止同特征值的连接在异步任务的情况下通过去重验证
    __lock = Lock()

    def __init__(self, max_connections=None, **kwargs):
        """
        初始配置
        :param max_connections:(type=int) 连接池允许的最大连接数，None表示采用内置限制连接数，默认采用内置限制
        :param kwargs:(type=dict) 其余的命名参数，用于接收数据库连接信息，如host、port等
        """

        # 获取配置信息
        host = kwargs.get('host', 'localhost')
        port = kwargs.get('port', 6379)
        db = kwargs.get('db', 0)

        # 校验连接复用性
        with Redis.__lock:
            self.__filter_repetition(host, port, db)

        # 校验通过，创建连接池
        self.__pool = redis.ConnectionPool(host=host, port=port, db=db, decode_responses=True,
                                           max_connections=max_connections)

    @classmethod
    def __filter_repetition(cls, host, port, db):
        """
        校验相同配置的连接是否已经创建
        :param host:(type=str) 数据库ip地址
        :param port:(type=int) 数据库端口
        :param db:(type=str) 数据库名
        """

        # 转换host
        if host.lower() == 'localhost' or host.startswith('127.'):
            host = 'localhost'

        # 根据连接信息计算特征值
        fp = cf.calculate_fp([host, str(port), str(db)])

        # 判断特征值是否已经存在
        # 存在则不给创建并抛异常，不存在则添加特征值用于后续判断
        if fp in cls.__filter_container:
            raise RepetitiveConnect
        else:
            cls.__filter_container.add(fp)

    def set(self, key, value, ex=None):
        """
        设置一对string类型数据的键值
        :param key:(type=str) 键
        :param value:(type=str) 值
        :param ex:(type=int) 过期时间（单位：秒），默认不过期
        :return result:(type=bool) 设置成功为True，否则为False
        """

        with redis.StrictRedis(connection_pool=self.__pool) as connection:
            try:
                result = connection.set(key, value, ex=ex)
            except redis.exceptions.ConnectionError as e:
                raise ConnectFailed(str(e))
        return result

    def get(self, key):
        """
        根据键获取string类型数据的值
        :param key:(type=str) 键
        :return result:(type=str,None) 值，没有结果则返回None
        """

        with redis.StrictRedis(connection_pool=self.__pool) as connection:
            try:
                result = connection.get(key)
            except redis.exceptions.ConnectionError as e:
                raise ConnectFailed(str(e))
        return result
