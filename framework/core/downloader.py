"""
下载器组件：
1.根据请求对象，发起请求（网络请求、数据库查询请求等），拿到数据，构建响应对象并返回
"""

from framework.object.response import Response
from framework.error.check_error import ParameterError, LackParameter
from utils import common_function as cf


class Downloader(object):
    """
    下载器组件
    """

    @staticmethod
    def __web(request):
        """
        发起网络请求，获取响应数据
        1.内置请求对象里，必须带上url参数，是请求的地址
        2.其中一个可选参数为is_json，为True则返回响应体体解析json后的数据，否则返回原生响应对象，默认True
        3.其余可选参数请查看工具包对应函数
        :param request:(type=Request) 即将发起请求的请求对象
        :return response:(type=外置Response,dict,list) 发起网络请求获取到的响应数据
        """

        # 获取下载信息
        kwargs = request.kwargs

        # 校验有没有url参数
        if kwargs.get('url') is None:
            raise LackParameter(['url'])

        # 发起请求，返回响应
        # 根据json参数确定是返回原生响应对象还是响应体解析json后的数据
        response = cf.repetition_json(**kwargs) if kwargs.get('is_json', True) else cf.request_get_response(**kwargs)
        return response

    def get_response(self, request):
        """
        发起请求获取响应
        :param request:(type=Request) 即将发起请求的请求对象
        :return response:(type=Response) 发起请求后获得的响应对象
        """

        # 1.根据请求对象，发起请求，获取响应
        if request.way.lower() == 'web':
            data = self.__web(request)
        elif request.way.lower() == 'db':
            data = None
        elif request.way.lower() == 'file':
            data = None
        elif request.way.lower() == 'test':
            print('这是一个彩蛋๑乛◡乛๑，业务名称为%s。' % request.builder_name)
            data = None
        else:
            raise ParameterError('way', ['“web”（获取网络数据）', '“db”（获取数据库数据）', '“file”（获取本地文件数据）'])

        # 2.构建响应对象，并返回
        response = Response(data)
        return response
