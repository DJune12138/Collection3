"""
MySQL数据库连接池
1.连接池适用于并发，PooledDB接口可用于多线程公用连接池
2.创建连接池后，每次执行语句只需要直接从池中获取空闲连接即可，用完关闭
3.每次获取和关闭连接并不是正真创建和销毁数据库连接，而是经过池化技术管理，省去创建和销毁连接的开销，提高效率
4.创建连接池时并没有真正连接数据库，如果业务里没有使用到对应数据库的话并不会浪费数据库连接的资源
"""

import pymysql
from DBUtils.PooledDB import PooledDB
from threading import Lock
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

    def __init__(self, sql, args, e):
        """
        初始配置
        :param sql:(type=str) 执行的SQL语句
        :param args:(type=None,tuple,list,dict) args参数
        :param e:(type=Exception) 原生报错对象
        """

        self.sql = sql
        self.sql_args = args
        self.e = e

    def __str__(self):
        """
        异常描述信息
        :return info:(type=str) 异常描述
        """

        info = 'SQL执行失败！\n原生报错信息：{}\nSQL：{}\nargs：{}'.format(str(self.e), self.sql, str(self.sql_args))
        return info


class RepetitiveConnect(MySQLError):
    """
    重复连接
    """

    def __str__(self):
        """
        异常描述信息
        :return info:(type=str) 异常描述
        """

        info = '相同配置的MySQL连接已创建！请勿重复创建连接，造成资源浪费！（Tips：如不同业务用同配置的连接，可在%s模块的MySQL配置里添加）' % account_name
        return info


class ConnectFailed(MySQLError):
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

        info = 'MySQL连接失败，请确认配置连接信息和数据库权限是否正确！%s' % self.info
        return info


class MySQL(object):
    """
    MySQL数据库连接池
    """

    # 去重容器，存储已经成功连接的配置信息的特征值
    # 配置信息为host、port、user、db
    __filter_container = set()

    # 互斥锁，防止同特征值的连接在异步任务的情况下通过去重验证
    __lock = Lock()

    def __init__(self, max_connections=None, set_session=None, **kwargs):
        """
        初始配置
        :param max_connections:(type=int) 连接池允许的最大连接数，0和None表示不限制连接数，默认不限制
        :param set_session:(type=list) 开始会话前执行的命令列表，如：["set datestyle to ...", "set time zone ..."]，默认为空
        :param kwargs:(type=dict) 其余的关键字参数，用于接收数据库连接信息，如host、port等
        """

        # 获取配置信息
        try:
            host = kwargs.get('host', 'localhost')
            port = kwargs.get('port', 3306)
            user = kwargs['user']
            db = kwargs['db']
            password = kwargs['password']
            charset = kwargs.get('charset', 'utf8')
        except KeyError as e:
            raise MySQLError('MySQL连接初始化失败！缺少必要的数据库连接信息：%s' % str(e).replace("'", ''))

        # 校验连接复用性
        with MySQL.__lock:
            self.__filter_repetition(host, port, user, db)

        # 校验通过，创建连接池
        self.__pool = PooledDB(creator=pymysql, autocommit=True, maxconnections=max_connections, setsession=set_session,
                               host=host, port=port, user=user, passwd=password, db=db, charset=charset)

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

    @staticmethod
    def __replace_value(value):
        """
        处理SQL语句中的特殊符号，不处理的话会出现SQL语法错误的坑
        :param value:(type=str,int) 要处理的值，原则上应该只要str或int
        :return value:(type=str) 处理过后的值
        """

        value = str(value).replace('"', '""').replace('\\', r'\\')
        return value

    def execute(self, sql, args=None, many=False, fetchall=True, debug=False, **kwargs):
        """
        执行SQL语句，常规增删改查可使用对应方法，自编写语句可直接使用此方法
        :param sql:(type=str) 要执行的SQL语句，一般用于执行常规增删改查之外的语句
        :param args:(type=tuple,list,dict) pymysql自带args，类似自动拼接SQL字符串并处理一些特殊符号的功能，默认None则不使用
        :param many:(type=bool) 是否使用executemany，一次执行多条SQL语句提高效率，默认不使用
        :param fetchall:(type=bool) 仅SELECT语句有效，是否返回查到的所有数据，True返回所有，False返回第一条，默认返回所有
        :param debug:(type=bool) 是否打印SQL语句以供调试，默认False则不打印
        :param kwargs:(type=dict) 额外的关键字参数，主要用于防止传入过多参数报错
        :return result:(type=list,dict,int) 执行结果，如果是SELECT语句则根据fetchall返回多条或单条数据，其他语句则返回受影响行数
        """

        # 1.从池中获取连接
        # 由于使用池化技术，每次执行语句都从池中获取一条空闲连接即可
        try:
            connection = self.__pool.connection()
        except pymysql.err.OperationalError as e:
            raise ConnectFailed(str(e))
        cursor = connection.cursor(cursor=pymysql.cursors.DictCursor)  # 带上这个cursor参数可以使查询结果返回list与dict

        # 2.执行SQL
        # 该对象固定auto_commit为True，连接池会自动提交事务
        if debug:
            cf.print_log(sql)
        try:
            if not many:
                result = cursor.execute(sql, args=args)
            else:
                result = cursor.executemany(sql, args)
        except Exception as e:
            raise ExecuteError(sql, args, e)

        # 3.判断是否为SELECT语句，如果是则获取查询结果
        if sql.lstrip()[:6].lower() == 'select':
            result = cursor.fetchall() if fetchall else cursor.fetchone()

        # 4.把连接放回池
        # 池化技术并不会正真销毁连接
        connection.close()

        # 5.返回执行结果
        return result

    def select(self, table, columns=None, after_table='', fetchall=True, debug=False, **kwargs):
        """
        拼接常规SELECT语句并执行，返回查询结果
        :param table:(type=str) 要查询数据的表名
        :param columns:(type=list) 要查询的字段，默认查所有字段
        :param after_table:(type=str) 表名后的语句，where、group by等，自由发挥，请自行遵守语法，默认为空
        :param fetchall:(type=bool) 是否返回查到的所有数据，True为是并返回一个列表，False则只返回第一条数据，默认返回所有
        :param debug:(type=bool) 是否打印SQL语句以供调试，默认False则不打印
        :param kwargs:(type=dict) 额外的关键字参数，主要用于防止传入过多参数报错
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
              % (columns,
                 table,
                 after_table)

        # 执行并返回查询结果
        result = self.execute(sql, fetchall=fetchall, debug=debug)
        return result

    def insert(self, table, values, columns=None, duplicates=None, ignore=False, dup_ac=False, limit_line=None,
               debug=False, **kwargs):
        """
        拼接常规INSERT语句并执行
        :param table:(type=str) 要插入数据的表名
        :param values:(type=list) 单条数据，["a", "b", "c", ...]；多条数据，[["a", "b", "c", ...], [1, 2, 3, ...], ...]
        :param columns:(type=list) 需要插入数据的字段，默认所有字段，["column1", "column2", "column3", ...]
        :param duplicates:(type=list) 唯一键冲突则更新，["column1", "column2", "column3", ...]，带上“::”则使用判断，详见对应注释
        :param ignore:(type=bool) 唯一键冲突则忽略，默认False；如果duplicates不为None，则ignore强制为False
        :param dup_ac:(type=bool) 适配自动采集流程用，会判断更新冲突数据，duplicates不为None才有效，默认False不启用
        :param limit_line:(type=int) 在插入多条数据时可启用，避免一次插入过多数据，每次分批插入几条，默认None则不启用
        :param debug:(type=bool) 是否打印SQL语句以供调试，默认False则不打印
        :param kwargs:(type=dict) 额外的关键字参数，主要用于防止传入过多参数报错
        :return result:(type=int) 执行结果，受影响行数
        """

        # 拼接字段
        if columns is not None:
            if not isinstance(columns, list):
                raise MySQLError('columns参数类型应该为list！')
            columns_sql = '(%s)' % ','.join(columns)
        else:
            columns_sql = ''

        # 校验values
        values_len = len(values)
        if not isinstance(values, list):
            raise MySQLError('values参数类型应该为list！')
        if values_len == 0:
            cf.print_log('values为空！跳过本次插入MySQL！')
            return

        # 拼接插入值
        if isinstance(values[0], list):  # 插入多条数据
            if limit_line is None:  # 不分批插入
                values_sql = ','.join(['(%s)' % ','.join(['"%s"' % self.__replace_value(value) for value in one_data])
                                       for one_data in values])
            else:  # 分批插入
                values_sql = ','.join(['(%s)' % ','.join(['"%s"' % self.__replace_value(value) for value in one_data])
                                       for one_data in values[:limit_line]])  # 第一批
                i_range = int(values_len / limit_line if not values_len % limit_line else values_len / limit_line + 1)
                for i in range(i_range):
                    if i == 0:
                        continue  # 第一批直接交由本次函数插入
                    i_values = values[i * limit_line: (i + 1) * limit_line]
                    self.insert(table, i_values, columns=columns, duplicates=duplicates, ignore=ignore, dup_ac=dup_ac,
                                debug=debug)  # 后续多次调用insert方法实现分批插入
        else:  # 插入单条数据
            values_sql = '(%s)' % ','.join(['"%s"' % self.__replace_value(value) for value in values])

        # 拼接“唯一键冲突则更新”
        # 元素里带上“::”则使用判断条件来更新，如“column::>=”则为新数值大于旧数值才更新
        # >、>=、<、<=可直接代入，其余为自定义条件
        if duplicates is not None:
            if not isinstance(duplicates, list):
                raise MySQLError('duplicates参数类型应该为list！')
            if dup_ac:
                duplicates_sql = 'ON DUPLICATE KEY UPDATE %s' % ','.join(
                    ['%s=IF(%s>VALUES(%s) AND %s=DATE_FORMAT(VALUES(%s),"%%Y-%%m-%%d"),VALUES(%s),%s)' % (
                        duplicate, duplicate, duplicate, duplicate.replace('time', 'date'), duplicate, duplicate,
                        duplicate) for duplicate in duplicates])
            else:
                duplicate_list = list()
                for duplicate in duplicates:
                    ds = duplicate.split('::')
                    if len(ds) == 1:
                        dup_str = '%s=VALUES(%s)' % (duplicate, duplicate)
                    else:
                        column_key, if_key = ds[0], ds[1]
                        if if_key in ('>', '>=', '<', '<='):
                            dup_str = '%s=IF(VALUES(%s)%s%s,VALUES(%s),%s)' \
                                      % (column_key, column_key, if_key, column_key, column_key, column_key)
                        else:
                            dup_str = '%s=IF(%s,VALUES(%s),%s)' % (column_key, if_key, column_key, column_key)
                    duplicate_list.append(dup_str)
                duplicates_sql = 'ON DUPLICATE KEY UPDATE %s' % ','.join(duplicate_list)
            ignore = False  # ignore强制为False
        else:
            duplicates_sql = ''

        # 拼接“唯一键冲突则忽略”
        ignore = ' IGNORE ' if ignore else ' '

        # 构造SQL
        sql = """INSERT%sINTO %s
        %s
        VALUES %s
        %s;""" \
              % (ignore, table,
                 columns_sql,
                 values_sql,
                 duplicates_sql)

        # 执行并返回受影响行数
        result = self.execute(sql, debug=debug)
        return result
