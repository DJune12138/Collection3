"""
得到的（日志）数据封装成响应对象（response）
"""


class Response(object):
    """
    响应对象
    """

    def __init__(self, meta=None):
        """
        初始配置
        :param meta:(type=∞) 用于请求对象与响应对象之间互传信息（数据），类型不限
        """

        self.meta = meta
