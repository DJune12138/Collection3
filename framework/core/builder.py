"""
建造器组件：
1.构建请求信息，生成请求对象
2.解析响应对象，返回数据对象或者新的请求对象
"""

from framework.object.request import Request
from framework.object.item import Item
from framework.error.check_error import CheckUnPass


class Builder(object):
    """
    建造器组件
    """

    # 建造器名称
    # 继承类后重写该属性即可，必须重写
    # 用于识别业务，每个业务应为唯一名称，必须使用字符串
    name = None

    # 默认初始请求
    # 继承类后重写该属性即可，必须为list类型
    # 目前只有start_requests方法会调用该属性，如继承类后重写了start_requests方法，可以不需要该属性
    # 元素必须为dict类型，且必须有“way”这对键值
    # 原生start_requests函数会把列表中每一个mapping作为关键字参数构建请求对象
    start = [{'way': 'test', 'parse': '_funny'}]

    def _funny(self, response):
        """
        彩蛋流程专用，这个函数一般只用于彩蛋，不用于继承重写或参与业务
        """

        item = self.item(None, parse='_funny')
        yield item

    def start_requests(self):
        """
        1.构建初始请求对象并返回
        2.继承后，该方法可以重写，但必须使用关键字yield内置请求对象
        :return request:(type=Request) 初始请求对象
        """

        if not isinstance(self.start, list):
            raise CheckUnPass('建造器的start必须为list类型！')
        for info in self.start:
            if not isinstance(info, dict) or 'way' not in info.keys():
                raise CheckUnPass('建造器的start列表里元素必须为dict类型，且必须有“way”这对键值！')
            request = self.request(**info)
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

        item = self.item(None)
        yield item

    @staticmethod
    def request(*args, **kwargs):
        """
        为业务建造器提供生成内置请求对象的接口
        :param args:(type=tuple) 生成请求对象的信息，可变参数
        :param kwargs:(type=dict) 生成请求对象的信息，关键字参数
        :return request:(type=Request) 内置请求对象
        """

        request = Request(*args, **kwargs)
        return request

    @staticmethod
    def item(*args, **kwargs):
        """
        为业务建造器提供生成内置数据对象的接口
        :param args:(type=tuple) 生成数据对象的信息，可变参数
        :param kwargs:(type=dict) 生成数据对象的信息，关键字参数
        :return item:(type=Item) 内置数据对象
        """

        item = Item(*args, **kwargs)
        return item
