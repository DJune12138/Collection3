"""
建造器组件：
1.构建请求信息，生成请求对象
2.解析响应对象，返回数据对象或者新的请求对象
"""

from framework.object.request import Request
from framework.object.item import Item


class Builder(object):
    """
    建造器组件
    """

    # 默认初始请求
    start = 'web'

    def start_requests(self):
        """
        构建初始请求对象并返回
        :return request:(type=Request) 初始请求对象
        """

        request = Request(self.start)
        return request

    def parse(self, response):
        """
        解析响应并返回新的请求对象或者数据对象
        :param response:(type=Response) 引擎回传的响应对象
        :return item:(type=Item) 分析处理后的数据对象
        """

        # TODO 后续会根据解析返回request对象
        item = Item(response)
        return item
