"""
MySQL数据库连接池
1.连接池适用于并发，PooledDB接口可用于多线程公用连接池
2.创建连接池后，每次执行语句只需要直接从池中获取空闲连接即可，用完关闭
3.每次获取和关闭连接并不是正真创建和销毁数据库连接，而是经过池化技术管理，省去创建和销毁连接的开销，提高效率
"""

import pymysql
from DBUtils.PooledDB import PooledDB
from utils import common_function as cf
from config import account_name


class MySQLError(Exception):
    """
    MySQL数据库连接池的异常类基类
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


class ExecuteError(MySQLError):
    """
    执行SQL语句时报错
    """

    def __init__(self, sql, row_msg):
        """
        初始配置
        :param sql:(type=str) 执行的SQL语句
        :param row_msg:(type=str) 原生报错信息
        """

        self.sql = sql
        self.row_msg = str(row_msg)

    def __str__(self):
        """
        异常描述信息
        :return info:(type=str) 异常描述
        """

        info = 'SQL执行失败！\n原生报错信息：%s\nSQL：%s' % (self.row_msg, self.sql)
        return info


class RepetitiveConnect(MySQLError):
    def __str__(self):
        """
        异常描述信息
        :return info:(type=str) 异常描述
        """

        info = '相同配置的MySQL连接已创建！请勿重复创建连接，造成资源浪费！（Tips：如不同业务用同配置的连接，可在%s模块的MySQL配置里添加）' % account_name
        return info


class MySQL(object):
    """
    MySQL数据库连接池
    """

    # 去重容器，存储已经成功连接的配置信息的特征值
    # 配置信息为host、port、user、db
    __filter_container = set()

    def __init__(self, max_connections=None, set_session=None, **kwargs):
        """
        初始配置
        :param max_connections:(type=int) 连接池允许的最大连接数，0和None表示不限制连接数，默认不限制
        :param set_session:(type=list) 开始会话前执行的命令列表，如：["set datestyle to ...", "set time zone ..."]，默认为空
        :param kwargs:(type=dict) 其余的命名参数，用于接收数据库连接信息，如ip、port等
        """

        # 获取配置信息
        try:
            host = kwargs.get('host', 'localhost')
            port = kwargs.get('port', 3306)
            user = kwargs['user']
            db = kwargs['db']
            password = kwargs['password']
            charset = kwargs.get('charset', 'utf-8')
        except KeyError as e:
            raise MySQLError('MySQL连接初始化失败！缺少必要的数据库连接信息：%s' % str(e).replace("'", ''))

        # 校验连接复用性
        self.__filter_repetition(host, port, user, db)

        # 校验通过，创建连接
        try:
            self.__pool = PooledDB(creator=pymysql, autocommit=True, mincached=1, maxconnections=max_connections,
                                   setsession=set_session, host=host, port=port, user=user, passwd=password, db=db,
                                   charset=charset)
        except pymysql.err.OperationalError as e:
            raise MySQLError('MySQL连接初始化失败，请确认配置连接信息和数据库权限是否正确！%s' % e)

    @classmethod
    def __filter_repetition(cls, host, port, user, db):
        """
        校验相同配置的连接是否已经创建
        :param host:(type=str) 数据库ip地址
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

    @staticmethod
    def __replace_value(value):
        """
        处理SQL语句中的特殊符号，不处理的话会出现SQL语法错误的坑
        :param value:(type=str,int) 要处理的值，原则上应该只要str或int
        :return value:(type=str) 处理过后的值
        """

        value = str(value).replace('"', '""').replace('\\', r'\\')
        return value

    def execute(self, sql, fetchall=True):
        """
        执行SQL语句，常规增删改查可使用对应方法，自编写语句可直接使用此方法
        :param sql:(type=str) 要执行的SQL语句，一般用于执行常规增删改查之外的语句
        :param fetchall:(type=bool) 仅SELECT语句有效，是否返回查到的所有数据，True返回所有，False返回第一条，默认返回所有
        :return result:(type=list,dict,int) 执行结果，如果是SELECT语句则根据fetchall返回多条或单条数据，其他语句则返回受影响行数
        """

        # 1.从池中获取连接
        # 由于使用池化技术，每次执行语句都从池中获取一条空闲连接即可
        connection = self.__pool.connection()
        cursor = connection.cursor(cursor=pymysql.cursors.DictCursor)  # 带上这个cursor参数可以使查询结果返回list与dict

        # 2.执行SQL
        # 该对象固定auto_commit为True，连接池会自动提交事务
        try:
            result = cursor.execute(sql)
        except Exception as e:
            raise ExecuteError(sql, str(e))

        # 3.判断是否为SELECT语句，如果是则获取查询结果
        if sql.lstrip()[:6].lower() == 'select':
            result = cursor.fetchall() if fetchall else cursor.fetchone()

        # 4.把连接放回池
        # 池化技术并不会正真销毁连接
        connection.close()

        # 5.返回执行结果
        return result

    def select(self, table, columns=None, after_table='', fetchall=True):
        """
        拼接常规SELECT语句并执行，返回查询结果
        :param table:(type=str) 要查询数据的表名
        :param columns:(type=list) 要查询的字段，默认查所有字段
        :param after_table:(type=str) 表名后的语句，where、group by等，自由发挥，请自行遵守语法，默认为空
        :param fetchall:(type=bool) 是否返回查到的所有数据，True为是并返回一个列表，False则只返回第一条数据，默认返回所有
        :return result:(type=list,dict) 查询结果，根据fetchall返回多条数据或单条数据
        """

        # 拼接字段
        if columns is not None:
            if not isinstance(columns, list):
                raise MySQLError('columns参数类型应该为list！')
            columns = ','.join(columns)
        else:
            columns = '*'

        # 构造SQL
        sql = """SELECT %s
        FROM %s
        %s;""" \
              % ('*' if columns is None else ','.join(columns),
                 table,
                 after_table)

        # 执行并返回查询结果
        result = self.execute(sql, fetchall=fetchall)
        return result

    def insert(self, table, values, columns=None, duplicates=None):
        """
        拼接常规INSERT语句并执行
        :param table:(type=str) 要插入数据的表名
        :param values:(type=list) 单条数据，["a", "b", "c", ...]；多条数据，[["a", "b", "c", ...], [1, 2, 3, ...], ...]
        :param columns:(type=list) 需要插入数据的字段，默认所有字段，["column1", "column2", "column3", ...]
        :param duplicates:(type=list) 主键冲突则更新，["column1", "column2", "column3", ...]
        :return result:(type=int) 执行结果，受影响行数
        """

        # 拼接字段
        if columns is not None:
            if not isinstance(columns, list):
                raise MySQLError('columns参数类型应该为list！')
            columns = '(%s)' % ','.join(columns)
        else:
            columns = ''

        # 拼接插入值
        if not isinstance(values, list):
            raise MySQLError('values参数类型应该为list！')
        if isinstance(values[0], list):  # 插入多条数据
            values = ','.join(['(%s)' % ','.join(['"%s"' % self.__replace_value(value) for value in one_data])
                               for one_data in values])
        else:  # 插入单条数据
            values = '(%s)' % ','.join(['"%s"' % self.__replace_value(value) for value in values])

        # 拼接“主键冲突则更新”
        if duplicates is not None:
            if not isinstance(duplicates, list):
                raise MySQLError('duplicates参数类型应该为list！')
            duplicates = 'ON DUPLICATE KEY UPDATE %s' % ','.join(
                ['%s=VALUES(%s)' % (duplicate, duplicate) for duplicate in duplicates])
        else:
            duplicates = ''

        # 构造SQL
        sql = """INSERT INTO %s
        %s
        VALUES
        %s
        %s;""" \
              % (table,
                 columns,
                 values,
                 duplicates)

        # 执行并返回受影响行数
        result = self.execute(sql)
        return result

    # TODO 改
    def update(self):
        pass

    # TODO 删
    def delete(self):
        pass
