"""
请求获得或分析计算后的数据封装成数据对象（item）
"""


class Item(object):
    """
    数据对象
    """

    def __init__(self, data, parse='process_item'):
        """
        初始配置
        :param data:(type=∞) 建造器分析处理好的数据
        :param parse:(type=str) 业务管道中解析该数据对象的解析函数的函数名，默认process_item函数
        """

        self.__data = data
        self.parse = parse

    @property
    def data(self):
        """
        对外提供data进行访问，一定程度达到保护的作用
        :return data:(type=Request,Response) 构造器解析后的数据
        """

        data = self.__data
        return data
