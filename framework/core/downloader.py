"""
下载器组件：
1.根据请求对象，发起请求（网络请求、数据库查询请求等），拿到数据，构建响应对象并返回
"""

from threading import Lock
from time import sleep
from framework.object.response import Response
from framework.error.check_error import ParameterError, LackParameter, CheckUnPass
from utils import common_function as cf
from services import mysql, redis, clickhouse


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
                limit_s = kwargs.get('limit_s', 0)
                if kwargs.get('first_pass', True):
                    if web_limit not in self.web_first:
                        self.web_first.add(web_limit)
                    else:
                        sleep(limit_s)
                else:
                    sleep(limit_s)

        # 发起请求，返回响应
        # 根据json参数确定是返回原生响应对象还是响应体解析json后的数据
        response = cf.repetition_json(**kwargs) if kwargs.get('is_json', True) else cf.request_get_response(**kwargs)
        return response

    @staticmethod
    def __db(kwargs):
        """
        获取数据库数据
        1.下载信息里，db_type为数据库类型，默认使用MySQL
        2.db_name对应account模块里对应数据库连接信息的json字符串的key
        3.当db_type为redis时，redis_get为获取数据的方式，默认get
        3.其余可选参数请查看工具包对应函数
        :param kwargs:(type=dict) 下载信息
        :return result:(type=list,dict) 查询结果
        """

        # 根据db_type，获取对应数据库数据
        db_type = kwargs.get('db_type', 'mysql')
        db_name = kwargs.get('db_name')
        if db_type == 'mysql':
            mysql_db = mysql[db_name]
            if kwargs.get('sql') is None:  # 根据有没有SQL语句，决定用什么函数
                result = mysql_db.select(**kwargs)
            else:
                result = mysql_db.execute(**kwargs)
        elif db_type == 'redis':
            redis_db = redis[db_name]
            result = getattr(redis_db, kwargs.get('redis_get', 'get'))(**kwargs)
        elif db_type == 'clickhouse':
            clickhouse_db = clickhouse[db_name]
            if kwargs.get('sql') is None:
                result = clickhouse_db.select(**kwargs)
            else:
                result = clickhouse_db.execute(**kwargs)
        else:
            raise ParameterError('db_type', ['mysql', 'redis', 'clickhouse'])

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
        with open(file_name, encoding=encoding) as f:
            result = f.readlines()

        # 根据传参行数返回数据
        result = result[lines - 1:]  # Tips：py索引从0开始，文件行数对应减1
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
        elif way == 'test':
            data = kwargs.get('test_data')
        else:
            raise ParameterError('way', ['“web”（获取网络数据）', '“db”（获取数据库数据）', '“shell”（执行shell命令并获取返回数据）',
                                         '“file”（获取日志文件数据）'])

        # 2.构建响应对象，并返回
        response = Response(data)
        return response
