"""
自定义异常类，基类
"""


class BaseError(Exception):
    """
    基类
    """

    def __init__(self, info=None):
        """
        初始配置
        :param info:(type=str) 报错提示信息，默认无
        """

        self.__info = info

    def __str__(self):
        """
        异常描述信息
        :return info:(type=str) 异常描述
        """

        info = self.__info if self.__info is not None else super().__str__()
        return info
