"""
SQLServer数据库连接池
1.说明基本与MySQL一致，但由于SQLServer目前基本只用在单个业务，暂不校验重复连接
"""

import pymssql
from DBUtils.PooledDB import PooledDB
from utils import common_function as cf


class SQLServerError(Exception):
    """
    SQLServer数据库连接的异常类基类
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


class ExecuteError(SQLServerError):
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


class ConnectFailed(SQLServerError):
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

        info = 'SQLServerError连接失败，请确认配置连接信息和数据库权限是否正确！%s' % self.info
        return info


class SQLServer(object):
    """
    SQLServer数据库连接
    """

    def __init__(self, **kwargs):
        """
        初始配置
        :param kwargs:(type=dict) 关键字参数，用于接收数据库连接信息，如host、user、password等
        """

        # 获取配置信息
        try:
            host = kwargs['host']
            user = kwargs['user']
            password = kwargs['password']
        except KeyError as e:
            raise SQLServerError('SQLServer连接初始化失败！缺少必要的数据库连接信息：%s' % str(e).replace("'", ''))
        database = kwargs.get('database')
        charset = kwargs.get('charset', 'utf8')

        # 创建连接
        self.__pool = PooledDB(creator=pymssql, host=host, user=user, password=password, database=database,
                               charset=charset, as_dict=True, autocommit=True)

    def execute(self, sql, debug=False, **kwargs):
        """
        执行SQL语句
        :param sql:(type=str) 要执行的SQL语句
        :param debug:(type=bool) 是否打印SQL语句以供调试，默认False则不打印
        :param kwargs:(type=dict) 额外的关键字参数，主要用于防止传入过多参数报错
        :return result:(type=list,None) 如果是SELECT语句则返回查询数据，否则返回None
        """

        # 1.从池中获取连接
        # 由于使用池化技术，每次执行语句都从池中获取一条空闲连接即可
        try:
            connection = self.__pool.connection()
        except Exception as e:
            raise ConnectFailed(str(e))
        cursor = connection.cursor()

        # 2.执行SQL
        # 该对象固定auto_commit为True，连接池会自动提交事务
        if debug:
            cf.print_log(sql)
        try:
            cursor.execute(sql)
        except Exception as e:
            raise ExecuteError(sql, e)

        # 3.判断是否为SELECT语句，如果是则获取查询结果
        result = None
        if sql.lstrip()[:6].lower() == 'select':
            result = cursor.fetchall()

        # 4.把连接放回池
        # 池化技术并不会正真销毁连接
        connection.close()

        # 5.返回执行结果
        return result
