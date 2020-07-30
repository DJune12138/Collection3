"""
得到的（日志）数据封装成响应对象（response）
"""


class Response(object):
    """
    响应对象
    """

    def __init__(self, data, meta=None):
        """
        初始配置
        :param data:(type=∞) 下载器获取成功的数据，类型不限
        :param meta:(type=∞) 用于请求对象与响应对象之间互传信息（数据），类型不限
        """

        self.__data = data
        self.meta = meta

    @property
    def data(self):
        """
        对外提供data进行访问，一定程度达到保护的作用
        :return data:(type=Request,Response) 构造器解析后的数据
        """

        data = self.__data
        return data
