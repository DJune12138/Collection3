"""
PostgreSQL数据库连接池
1.说明与MySQL基本一致，可参考MySQL说明
2.PostgreSQL连接时需要指定数据库，否则默认连接和用户名同名的数据库
"""

import psycopg2
from psycopg2.extras import RealDictCursor
from DBUtils.PooledDB import PooledDB
from threading import Lock
from utils import common_function as cf
from config import account_name


class PostgreSQLError(Exception):
    """
    PostgreSQL数据库连接池的异常类基类
    """

    def __init__(self, info=None):
        """
        初始配置
        :param info:(type=str) 报错提示信息，默认无
        """

        self.info = info

    def __str__(self):
        """
        异常描述信息
        :return info:(type=str) 异常描述
        """

        info = self.info if self.info is not None else super().__str__()
        return info


class ExecuteError(PostgreSQLError):
    """
    执行SQL语句时报错
    """

    def __init__(self, sql, e):
        """
        初始配置
        :param sql:(type=str) 执行的SQL语句
        :param e:(type=Exception) 原生报错对象
        """

        self.sql = sql
        self.e = e

    def __str__(self):
        """
        异常描述信息
        :return info:(type=str) 异常描述
        """

        info = 'SQL执行失败！\n原生报错信息：{}\nSQL：{}'.format(str(self.e), self.sql)
        return info


class RepetitiveConnect(PostgreSQLError):
    """
    重复连接
    """

    def __str__(self):
        """
        异常描述信息
        :return info:(type=str) 异常描述
        """

        info = '相同配置的PostgreSQL连接已创建！请勿重复创建连接，造成资源浪费！（Tips：如不同业务用同配置的连接，可在%s模块的PostgreSQL配置里添加）' % account_name
        return info


class ConnectFailed(PostgreSQLError):
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

        info = 'PostgreSQL连接失败，请确认配置连接信息和数据库权限是否正确！%s' % self.info
        return info


class PostgreSQL(object):
    """
    PostgreSQL数据库连接池
    """

    # 去重容器，存储已经成功连接的配置信息的特征值
    # 配置信息为host、port、user、database
    __filter_container = set()

    # 互斥锁，防止同特征值的连接在异步任务的情况下通过去重验证
    __lock = Lock()

    def __init__(self, **kwargs):
        """
        初始配置
        :param kwargs:(type=dict) 关键字参数，用于接收数据库连接信息，如host、port等
        """

        # 获取配置信息
        try:
            host = kwargs.get('host', 'localhost')
            port = kwargs.get('port', 5432)
            user = kwargs['user']
            password = kwargs['password']
            database = kwargs['database']
        except KeyError as e:
            raise PostgreSQLError('PostgreSQL连接初始化失败！缺少必要的数据库连接信息：%s' % str(e).replace("'", ''))

        # 校验连接复用性
        with PostgreSQL.__lock:
            self.__filter_repetition(host, port, user, database)

        # 校验通过，创建连接池
        self.__pool = PooledDB(creator=psycopg2, host=host, port=port, user=user, password=password, database=database,
                               cursor_factory=RealDictCursor)

    @classmethod
    def __filter_repetition(cls, host, port, user, db):
        """
        校验相同配置的连接是否已经创建
        :param host:(type=str) 数据库IP地址
        :param port:(type=int) 数据库端口
        :param user:(type=str) 登录数据库的账号名
        :param db:(type=str) 数据库名
        """

        # 转换host
        if host.lower() == 'localhost' or host.startswith('127.'):
            host = 'localhost'

        # 根据连接信息计算特征值
        fp = cf.calculate_fp([host, str(port), user, db])

        # 判断特征值是否已经存在
        # 存在则不给创建并抛异常，不存在则添加特征值用于后续判断
        if fp in cls.__filter_container:
            raise RepetitiveConnect
        else:
            cls.__filter_container.add(fp)

    def execute(self, sql, debug=False, **kwargs):
        """
        执行SQL语句，常规增删改查可使用对应方法，自编写语句可直接使用此方法
        :param sql:(type=str) 要执行的SQL语句，一般用于执行常规增删改查之外的语句
        :param debug:(type=bool) 是否打印SQL语句以供调试，默认False则不打印
        :param kwargs:(type=dict) 额外的关键字参数，主要用于防止传入过多参数报错
        :return result:(type=list,dict,int) 执行结果，如果是SELECT语句则根据fetchall返回多条或单条数据，其他语句则返回受影响行数
        """

        # 1.从池中获取连接
        # 由于使用池化技术，每次执行语句都从池中获取一条空闲连接即可
        try:
            connection = self.__pool.connection()
        except psycopg2.OperationalError as e:
            raise ConnectFailed(str(e))
        cursor = connection.cursor()

        # 2.执行SQL
        if debug:
            cf.print_log(sql)
        try:
            result = cursor.execute(sql)
        except Exception as e:
            raise ExecuteError(sql, e)

        # 3.判断是否为SELECT语句，如果是则获取查询结果
        if sql.lstrip()[:6].lower() == 'select':
            result = cursor.fetchall()

        # 4.提交事务
        connection.commit()

        # 5.把连接放回池
        # 池化技术并不会正真销毁连接
        connection.close()

        # 6.返回执行结果
        return result
