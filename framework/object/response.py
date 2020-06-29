"""
得到的（日志）数据封装成响应对象（response）
"""

import json


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

    @property
    def json(self):
        """
        解析响应对象的json字符串
        :return py_obj:(type=dict,list) 解析响应对象的json字符串，转为python对象
        """

        # py_obj = json.loads(self.body)  # TODO 待完善
        py_obj = {}
        return py_obj
