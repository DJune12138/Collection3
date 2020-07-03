"""
请求获得或分析计算后的数据封装成数据对象（item）
"""


class Item(object):
    """
    数据对象
    """

    def __init__(self, data):
        """
        初始配置
        :param data:(type=Request,Response) 构造器解析后的数据
        """

        self._data = data

    def __str__(self):
        """
        对象描述信息
        :return info:(type=str) 对象描述信息
        """

        info = '我是内置数据对象'
        return info

    @property
    def data(self):
        """
        对外提供data进行访问，一定程度达到保护的作用
        :return data:(type=Request,Response) 构造器解析后的数据
        """

        data = self._data
        return data
