"""
请求数据的方式封装成请求对象（request）
"""


class Request(object):
    """
    请求对象
    """

    def __init__(self, collect_way, parse='parse', meta=None):
        """
        初始配置
        :param collect_way:(type=str) 数据采集方式，限定“web”（获取网络数据）或“db”（获取数据库数据）
        :param parse:(type=str) 业务建造器中解析该请求对象的解析函数的函数名，默认parse函数
        :param meta:(type=∞) 用于请求对象与响应对象之间互传信息（数据）
        """

        self.collect_way = collect_way
        self.parse = parse
        self.meta = meta
