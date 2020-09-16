"""
下载器组件：
1.根据请求对象，发起请求（网络请求、数据库查询请求等），拿到数据，构建响应对象并返回
"""

from framework.object.response import Response
from framework.error.check_error import ParameterError, LackParameter
from utils import common_function as cf
from services import mysql


class Downloader(object):
    """
    下载器组件
    """

    @staticmethod
    def __web(kwargs):
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
        else:
            raise ParameterError('db_type', ['mysql'])

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
            data = None
        elif way == 'test':
            print('这是一个彩蛋๑乛◡乛๑，业务名称为%s。' % request.builder_name)
            data = None
        else:
            raise ParameterError('way', ['“web”（获取网络数据）', '“db”（获取数据库数据）', '“file”（获取本地文件数据）',
                                         '“shell”（执行shell命令并获取返回数据）'])

        # 2.构建响应对象，并返回
        response = Response(data)
        return response
