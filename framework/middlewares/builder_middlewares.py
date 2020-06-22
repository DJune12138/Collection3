"""
建造器中间件：
1.对请求对象和响应对象做预处理或者后续处理，相当于一个钩子
"""


class BuilderMiddleware(object):
    """
    建造器中间件基类
    """

    def process_request(self, request):
        """
        预处理请求对象
        :param request:(type=Request) 请求对象
        :return request:(type=Request) 加工后的请求对象
        """

        print('这是建造器中间件process_request方法')
        request = request
        return request

    def process_response(self, response):
        """
        预处理响应对象
        :param response:(type=Response) 响应对象
        :return response:(type=Response) 加工后的响应对象
        """

        print('这是建造器中间件process_response方法')
        response = response
        return response
