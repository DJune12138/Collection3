"""
下载器组件：
1.根据请求对象，发起请求（网络请求、数据库查询请求等），拿到数据，构建响应对象并返回
"""

from framework.object.response import Response


class Downloader(object):
    """
    下载器组件
    """

    def get_response(self, request):
        """
        发起请求获取响应
        :param request:(type=Request) 即将发起请求的请求对象
        :return response:(type=Response) 发起请求后获得的响应对象
        """

        # 1.根据请求对象，发起请求，获取响应
        if request.collect_way.lower() == 'web':
            print('start web')
        elif request.collect_way.lower() == 'db':
            print('start db')
        else:
            raise ValueError('collect_way只能为“web”（获取网络数据）或“db”（获取数据库数据）！')

        # 2.构建响应对象，并返回
        response = Response()
        return response
