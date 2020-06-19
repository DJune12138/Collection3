"""
请求（日志）数据的方式封装成请求对象（request）
"""


class Request(object):
    """
    请求对象
    """

    def __init__(self, collect_way):
        """
        初始配置
        :param collect_way:(type=[str]) 数据采集方式，限定“web”（获取网络数据）或“db”（获取数据库数据）
        """

        self.collect_way = collect_way
