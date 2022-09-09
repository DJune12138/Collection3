"""
下载器组件：
1.根据请求对象，发起请求（网络请求、数据库查询请求等），拿到数据，构建响应对象并返回
"""

from threading import Lock
from time import sleep
from lxml import etree
from framework.object.response import Response
from framework.error.check_error import ParameterError, LackParameter, CheckUnPass
from utils import common_function as cf
from utils.mongodb import mongodb_operation
from services import mysql, redis, clickhouse, postgresql


class Downloader(object):
    """
    下载器组件
    """

    def __init__(self):
        """
        下载器不用于继承，每次启动程序只有一个实例，可以直接在init实现初始化
        """

        # web方法限流用
        self.web_lock = dict()  # 存储线程锁
        self.web_first = set()  # 存储“首跳”

        # db方法防死锁用
        self.db_lock = dict()

    def __web(self, kwargs):
        """
        发起网络请求，获取响应数据
        1.下载信息里，必须带上url参数，是请求的地址
        2.其中一个可选参数为is_json，为True则返回响应体体解析json后的数据，否则返回原生响应对象，默认True
        3.其余可选参数请查看工具包对应函数
        :param kwargs:(type=dict) 下载信息
        :return response:(type=外置Response,dict,list) 发起网络请求获取到的响应数据
        """

        # 校验有没有url参数
        if kwargs.get('url') is None:
            raise LackParameter(['url'])

        # 限流保护机制
        # 1.如果请求对象带上web_limit参数，将启动限流保护机制
        # 2.web_limit应该为字符串，是一个唯一标识，建议带上业务名称，可进一步增加唯一性
        # 3.启动限流保护机制后，如first_pass为True，则第一次访问不会阻塞，默认True
        # 4.根据limit_s阻塞多少秒，应为float或int
        web_limit = kwargs.get('web_limit')
        if isinstance(web_limit, str):
            lock = self.web_lock.setdefault(web_limit, Lock())
            with lock:
                limit_s = kwargs.get('limit_s', 1)
                if kwargs.get('first_pass', True):
                    if web_limit not in self.web_first:
                        self.web_first.add(web_limit)
                    else:
                        sleep(limit_s)
                else:
                    sleep(limit_s)

        # 发起请求，返回响应，根据web_type参数返回不同类型的数据：
        # ① “json”返回解析json后的数据（默认）
        # ② “response”返回原生响应对象
        # ③ “xpath”返回Element对象
        # ④ “text”返回响应体文本str
        # ⑤ “csv”返回解析csv后的数据
        web_type = kwargs.get('web_type', 'json')
        if web_type == 'json':
            response = cf.repetition_json(**kwargs)
        elif web_type == 'response':
            response = cf.request_get_response(**kwargs)
        elif web_type == 'xpath':
            response = etree.HTML(cf.request_get_response(**kwargs).text)
        elif web_type == 'text':
            response = cf.request_get_response(**kwargs).text
        elif web_type == 'csv':
            response = cf.analyze_csv(cf.request_get_response(**kwargs).text)
        else:
            raise ParameterError('web_type', ['“json”（解析json后的数据）', '“response”（原生响应对象）', '“xpath”（Element对象）',
                                              '“text”（响应体文本str）', '“csv”（解析csv后的数据）'])
        return response

    def __db(self, kwargs):
        """
        获取数据库数据
        1.下载信息里，db_type为数据库类型，默认使用MySQL
        2.db_name对应account模块里对应数据库连接信息的json字符串的key
        3.当db_type为redis时，redis_get为获取数据的方式，默认get
        3.其余可选参数请查看工具包对应函数
        4.带上“db_limit”参数并且为str类型，则开启防并发执行功能
        :param kwargs:(type=dict) 下载信息
        :return result:(type=list,dict) 查询结果
        """

        # 根据db_type，获取对应数据库数据
        # 如db_type为“else”，则直接执行对应数据库对象的execute方法
        db_type = kwargs.get('db_type', 'mysql')
        db_object = kwargs.get('db_object')  # 传入一个数据库对象则使用该数据库
        db_name = kwargs.get('db_name')  # 传入一个name则使用配置数据库，前提是不传入db_object
        db_limit = kwargs.get('db_limit')
        lock = self.db_lock.setdefault(db_limit, Lock()) if isinstance(db_limit, str) else None
        if db_type == 'mysql':
            mysql_db = mysql[db_name] if db_object is None else db_object
            if kwargs.get('sql') is None:  # 根据有没有SQL语句，决定用什么函数
                if lock is not None:
                    with lock:
                        result = mysql_db.select(**kwargs)
                else:
                    result = mysql_db.select(**kwargs)
            else:
                if lock is not None:
                    with lock:
                        result = mysql_db.execute(**kwargs)
                else:
                    result = mysql_db.execute(**kwargs)
        elif db_type == 'clickhouse':
            clickhouse_db = clickhouse[db_name] if db_object is None else db_object
            if kwargs.get('sql') is None:
                if lock is not None:
                    with lock:
                        result = clickhouse_db.select(**kwargs)
                else:
                    result = clickhouse_db.select(**kwargs)
            else:
                if lock is not None:
                    with lock:
                        result = clickhouse_db.execute(**kwargs)
                else:
                    result = clickhouse_db.execute(**kwargs)
        elif db_type == 'redis':
            redis_db = redis[db_name] if db_object is None else db_object
            if lock is not None:
                with lock:
                    result = getattr(redis_db, kwargs.get('redis_get', 'get'))(**kwargs)
            else:
                result = getattr(redis_db, kwargs.get('redis_get', 'get'))(**kwargs)
        elif db_type == 'postgresql':
            postgresql_db = postgresql[db_name] if db_object is None else db_object
            if lock is not None:
                with lock:
                    result = postgresql_db.execute(**kwargs)
            else:
                result = postgresql_db.execute(**kwargs)
        elif db_type == 'mongodb':
            mg_args = kwargs.get('mg_args', tuple())  # 操作MongoDB的指令列表，详见mongodb_operation函数注释
            result = mongodb_operation(db_object, *mg_args)  # 直接使用mongodb对象
        elif db_type == 'else':
            if lock is not None:
                with lock:
                    result = db_object.execute(**kwargs)
            else:
                result = db_object.execute(**kwargs)
        else:
            raise ParameterError('db_type', ['mysql', 'clickhouse', 'redis', 'postgresql', 'mongodb'])

        # 返回数据
        return result

    @staticmethod
    def __shell(kwargs):
        """
        执行shell命令并获取返回数据
        :param kwargs:(type=dict) 下载信息
        :return result:(type=CompletedProcess) shell命令返回的结果，详细参数参考函数介绍里的网址
        """

        result = cf.shell_run(**kwargs)
        return result

    @staticmethod
    def __file(kwargs):
        """
        获取日志文件数据
        :param kwargs:(type=dict) 下载信息
        :return result:(type=list) 日志文件数据
        """

        # 获取传参数据
        file_name = kwargs.get('file_name')  # 文件名/路径
        if file_name is None:
            raise LackParameter(['file_name'])
        encoding = kwargs.get('encoding')  # 读取编码
        lines = int(kwargs.get('lines', 1))  # 从文件第几行开始返回数据，第一行意味全量返回
        if lines < 1:
            raise CheckUnPass('文件行数lines不能小于1！')

        # 读取文件，获取数据
        with open(file_name, encoding=encoding, errors='ignore') as f:
            result = f.readlines()

        # 根据传参行数返回数据
        result = result[lines - 1:]  # Tips：py索引从0开始，文件行数对应减1
        return result

    @staticmethod
    def __sdk(kwargs):
        """
        调用SDK获取数据
        1.sdk_fun为函数引用，必传
        2.sdk_args为调用函数时的位置参数，使用tuple，不传默认为空元组
        3.sdk_kwargs为调用函数时的关键字参数，使用dict，不传默认为空字典
        4.sdk_chained为链式调用，默认None则不启用，启用写法参照下方注释
        :param kwargs:(type=dict) 下载信息
        :return result:(type=∞) SDK调用结果
        """

        # 获取传参与调用SDK
        sdk_fun = kwargs.get('sdk_fun')
        if sdk_fun is None:
            raise LackParameter(['sdk_fun'])
        sdk_args = kwargs.get('sdk_args', tuple())
        sdk_kwargs = kwargs.get('sdk_kwargs', dict())
        result = sdk_fun(*sdk_args, **sdk_kwargs)

        # 链式调用
        # 该参数为list或tuple，里面的元素为dict，有以下键值：
        # 1.sdk_fun必选，为字符串对象，是调用函数名
        # 2.run可选，布尔值，默认True，True则执行函数，否则等效于获取属性
        # 3.sdk_args可选，与上方一致
        # 4.sdk_kwargs可选，与上方一致
        # 示例：fun1().fun2.fun3(1,b=2) ↓
        # [{"sdk_fun":"fun1"},{"sdk_fun":"fun2","run":False},{"sdk_fun":"fun3","sdk_args":(1,),"sdk_kwargs":{"b":2}}]
        sdk_chained = kwargs.get('sdk_chained')
        if sdk_chained:
            for one in sdk_chained:
                sdk_fun = one['sdk_fun']  # 方法名
                run = one.get('run', True)  # 是否需要执行的标记（执行即带括号）
                if run:
                    sdk_args = one.get('sdk_args', tuple())  # 位置参数
                    sdk_kwargs = one.get('sdk_kwargs', dict())  # 关键字参数
                    result = getattr(result, sdk_fun)(*sdk_args, **sdk_kwargs)
                else:
                    result = getattr(result, sdk_fun)
        return result

    def get_response(self, request):
        """
        发起请求获取响应
        :param request:(type=Request) 即将发起请求的请求对象
        :return response:(type=Response) 发起请求后获得的响应对象
        """

        # 1.根据请求方式，发起请求，获取响应
        way = request.way.lower()  # 请求方式
        kwargs = request.kwargs  # 下载信息
        if way == 'web':
            data = self.__web(kwargs)
        elif way == 'db':
            data = self.__db(kwargs)
        elif way == 'shell':
            data = self.__shell(kwargs)
        elif way == 'file':
            data = self.__file(kwargs)
        elif way == 'sdk':
            data = self.__sdk(kwargs)
        elif way == 'test':
            data = kwargs.get('test_data')
        else:
            raise ParameterError('way', ['“web”（获取网络数据）', '“db”（获取数据库数据）', '“shell”（执行shell命令并获取返回数据）',
                                         '“file”（获取日志文件数据）', '“sdk”（调用SDK获取数据）'])

        # 2.构建响应对象，并返回
        response = Response(data)
        return response
