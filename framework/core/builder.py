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

    # 建造器名称
    # 继承类后重写该属性即可
    # 用于识别业务，每个业务应为唯一名称，必须使用字符串
    name = ''

    # 默认初始请求
    # 继承类后重写该属性即可
    # 目前只有start_requests方法会调用该属性，如继承类后重写了start_requests方法，可以不需要该属性
    start = []

    def start_requests(self):
        """
        构建初始请求对象并返回
        :return request:(type=Request) 初始请求对象
        """

        for one in self.start:
            request = Request(one)
            yield request

    def parse(self, response):
        """
        解析响应并返回新的请求对象或者数据对象
        :param response:(type=Response) 引擎回传的响应对象
        :return item:(type=Item) 分析处理后的数据对象
        """

        item = Item(response)
        yield item
