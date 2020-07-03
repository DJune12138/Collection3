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
        1.构建初始请求对象并返回
        2.继承后，该方法可以重写，但必须使用关键字yield内置请求对象
        :return request:(type=Request) 初始请求对象
        """

        for info in self.start:
            request = self.request(info)
            yield request

    def parse(self, response):
        """
        1.解析响应并返回新的请求对象或者数据对象
        2.此方法大多数情况下会被业务建造器继承并重写，原生只是基本流通
        3.重写过后，必须使用关键字yield内置请求对象（送去调度器）或内置数据对象（送去管道）
        4.重写该方法必须要接收一个参数（无论业务使用与否），是响应对象
        :param response:(type=Response) 引擎回传的响应对象
        :return item:(type=Item,Request) 内置数据、请求对象
        """

        item = self.item(response)
        yield item

    @staticmethod
    def request(info):
        """
        为业务建造器提供生成内置请求对象的接口
        :param info:(type=) 请求信息（数据类型还没定好，待补）
        :return request:(type=Request) 内置请求对象
        """

        request = Request(info)
        return request

    @staticmethod
    def item(data):
        """
        为业务建造器提供生成内置数据对象的接口
        :param data:(type=∞) 建造器解析处理好的数据，虽然类型不限，但一般建议使用便于解读数据结构的类型，比如dict、list
        :return item:(type=Item) 内置数据对象
        """

        item = Item(data)
        return item
