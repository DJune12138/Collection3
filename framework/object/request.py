"""
请求数据的方式封装成请求对象（request）
"""


class Request(object):
    """
    请求对象
    """

    def __init__(self, way, parse='parse', meta=None, **kwargs):
        """
        初始配置
        :param way:(type=str) 数据采集方式，限定“web”（获取网络数据）或“db”（获取数据库数据）或“file”（获取本地文件数据）
        :param parse:(type=str) 业务建造器中解析该请求对象的解析函数的函数名，默认parse函数
        :param meta:(type=∞) 用于请求对象与响应对象之间互传信息（数据）
        :param kwargs:(type=dict) 提供给下载器的信息（URL、文件路径、数据库连接等）
        """

        self.way = way
        self.parse = parse
        self.meta = meta
        self.kwargs = kwargs
