"""
下载器中间件
"""


class DownloaderMiddleware(object):
    """
    下载器中间件基类
    """

    def process_request(self, request):
        """
        1.预处理请求对象
        2.继承后，可重写该方法，自行编写业务逻辑
        3.重写该方法必须要接收一个参数（无论业务使用与否），是请求对象
        4.重写后必须return一个内置请求对象
        :param request:(type=Request) 请求对象
        :return request:(type=Request) 加工后的请求对象
        """

        print('这是下载器中间件process_request方法')
        request = request
        return request

    def process_response(self, response):
        """
        1.预处理响应对象
        2.继承后，可重写该方法，自行编写业务逻辑
        3.重写该方法必须要接收一个参数（无论业务使用与否），是响应对象
        4.重写后必须return一个内置响应对象
        :param response:(type=Response) 响应对象
        :return response:(type=Response) 加工后的响应对象
        """

        print('这是下载器中间件process_response方法')
        response = response
        return response
