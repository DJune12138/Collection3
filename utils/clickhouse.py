"""
ClickHouse数据库连接池
1.说明与MySQL基本一致，可参考MySQL说明
2.clickhouse_driver文档：https://clickhouse-driver.readthedocs.io
"""

from clickhouse_driver import dbapi
from clickhouse_driver.dbapi.extras import DictCursor
from clickhouse_driver.dbapi.errors import OperationalError
from DBUtils.PooledDB import PooledDB
from threading import Lock
from config import account_name
from utils import common_function as cf


class ClickHouseError(Exception):
    """
    ClickHouse数据库连接池的异常类基类
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


class ExecuteError(ClickHouseError):
    """
    执行SQL语句时报错
    """

    def __init__(self, sql, parameters, e):
        """
        初始配置
        :param sql:(type=str) 执行的SQL语句
        :param parameters:(type=None,tuple,list,dict) params参数
        :param e:(type=Exception) 原生报错对象
        """

        self.sql = sql
        self.sql_parameters = parameters
        self.e = e

    def __str__(self):
        """
        异常描述信息
        :return info:(type=str) 异常描述
        """

        info = 'SQL执行失败！\n原生报错信息：{}\nSQL：{}\nparameters：{}'.format(str(self.e), self.sql, str(self.sql_parameters))
        return info


class RepetitiveConnect(ClickHouseError):
    """
    重复连接
    """

    def __str__(self):
        """
        异常描述信息
        :return info:(type=str) 异常描述
        """

        info = '相同配置的ClickHouse连接已创建！请勿重复创建连接，造成资源浪费！（Tips：如不同业务用同配置的连接，可在%s模块的ClickHouse配置里添加）' % account_name
        return info


class ConnectFailed(ClickHouseError):
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

        info = 'ClickHouse连接失败，请确认配置连接信息和数据库权限是否正确！%s' % self.info
        return info


class ClickHouse(object):
    """
    ClickHouse数据库连接池
    """

    # 去重容器，存储已经成功连接的配置信息的特征值
    # 配置信息为host、port、user、db
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
            port = kwargs.get('port', 9000)
            user = kwargs['user']
            password = kwargs['password']
            database = kwargs['database']
        except KeyError as e:
            raise ClickHouseError('ClickHouse连接初始化失败！缺少必要的数据库连接信息：%s' % str(e).replace("'", ''))

        # 校验连接复用性
        with ClickHouse.__lock:
            self.__filter_repetition(host, port, user, database)

        # 校验通过，创建连接池
        self.__pool = PooledDB(creator=dbapi, host=host, port=port, user=user, password=password, database=database)

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

    def execute(self, sql, fetchall=True, parameters=None, debug=False, **kwargs):
        """
        执行SQL语句，常规SELECT语句和INSERT语句可使用对应方法，自编写语句可直接使用此方法
        :param sql:(type=str) 要执行的SQL语句，一般用于执行常规增删改查之外的语句
        :param fetchall:(type=bool) SELECT语句使用，详见select方法
        :param parameters:(type=tuple,list,dict) INSERT语句与格式化SQL（防止SQL注入）使用，详见insert方法
        :param debug:(type=bool) 是否打印SQL语句以供调试，默认False则不打印
        :param kwargs:(type=dict) 额外的关键字参数，主要用于防止传入过多参数报错
        :return result:(type=list,dict,int) 执行结果，如果是SELECT语句则根据fetchall返回多条（list）或单条（dict）数据，其余返回SQL作用行数
        """

        # 从连接池中获取连接
        try:
            connection = self.__pool.connection()
        except OperationalError as e:
            raise ConnectFailed(str(e))
        cursor = connection.cursor(cursor_factory=DictCursor)

        # 执行SQL，如果为SELECT语句则result为查询结果，否则为SQL作用行数
        if debug:
            cf.print_log(sql)
        try:
            cursor.execute(sql, parameters=parameters)
        except OperationalError as e:
            raise ExecuteError(sql, parameters, e)
        if sql.lstrip().lower().startswith('select'):
            result = cursor.fetchall() if fetchall else cursor.fetchone()
        else:
            result = cursor.rowcount

        # 把连接放回连接池，并返回执行结果
        connection.close()
        return result

    def select(self, table, columns=None, after_table='', fetchall=True, debug=False, **kwargs):
        """
        拼接常规SELECT语句并执行，返回查询结果
        :param table:(type=str) 要查询数据的表名
        :param columns:(type=list) 要查询的字段，默认查所有字段
        :param after_table:(type=str) 表名后的语句，where、group by等，自由发挥，请自行遵守语法，默认为空
        :param fetchall:(type=bool) 是否返回查到的所有数据，True则返回一个列表，False则只返回第一条数据，默认True
        :param debug:(type=bool) 是否打印SQL语句以供调试，默认False则不打印
        :param kwargs:(type=dict) 额外的关键字参数，主要用于防止传入过多参数报错
        :return result:(type=list) 查询结果，每条数据为一个元组
        """

        # 拼接字段
        if columns is not None:
            if not isinstance(columns, list):
                raise ClickHouseError('columns参数类型应该为list！')
            columns = ','.join(columns)
        else:
            columns = '*'

        # 构造SQL，执行并返回查询结果
        sql = """SELECT %s FROM %s
        %s;""" % (columns, table, after_table)
        result = self.execute(sql, fetchall=fetchall, debug=debug)
        return result

    def insert(self, table, parameters, columns=None, limit_line=None, debug=False, **kwargs):
        """
        拼接常规INSERT语句并执行
        1.关于parameters参数，参考格式为[[v1, v2, ...], [v3, v4, ...], ...]，里面的数据需要根据数据表转成对应类型
        2.例如数据表的UInt类型要转成Python的int类型，String→str，DateTime→datetime，否则会执行出错
        3.详情可参考 https://clickhouse-driver.readthedocs.io/en/latest/quickstart.html#inserting-data
        :param table:(type=str) 要插入数据的表名
        :param parameters:(type=tuple,list,dict) 要插入的数据，详见execute函数的说明
        :param columns:(type=list) 需要插入数据的字段，默认所有字段，["column1", "column2", "column3", ...]
        :param limit_line:(type=int) 分批插入的条数，分批插入数据以避免一次插入过多数据，默认None则不启用
        :param debug:(type=bool) 是否打印SQL语句以供调试，默认False则不打印
        :param kwargs:(type=dict) 额外的关键字参数，主要用于防止传入过多参数报错
        :return result:(type=int) 执行结果，SQL作用行数
        """

        # 拼接字段
        source_columns = columns  # 分批插入时使用
        if columns is not None:
            if not isinstance(columns, list):
                raise ClickHouseError('columns参数类型应该为list！')
            columns = '(%s)' % ','.join(columns)
        else:
            columns = ''

        # 分批插入
        if limit_line:
            source_parameters = parameters  # 源数据另外保存
            parameters = source_parameters[:limit_line]  # 第一批
            len_par = len(source_parameters)
            i_range = int(len_par / limit_line if not len_par % limit_line else len_par / limit_line + 1)
            for i in range(i_range):
                if i == 0:
                    continue  # 第一批直接交由本次函数插入
                i_parameters = source_parameters[i * limit_line: (i + 1) * limit_line]
                self.insert(table, i_parameters, columns=source_columns, debug=debug)  # 后续多次调用insert方法实现分批插入

        # 构造SQL，执行并返回SQL作用行数
        sql = "INSERT INTO %s%s VALUES" % (table, columns)
        result = self.execute(sql, parameters=parameters, debug=debug)
        return result
