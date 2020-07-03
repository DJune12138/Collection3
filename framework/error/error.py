"""
自定义异常类
"""

from . import BaseError


class TypeDifferent(BaseError):
    """
    返回的对象类型错误，用于校验继承内置组件后返回的对象
    """

    def __init__(self, right_obj=None):
        """
        初始配置
        :param right_obj:(type=内置对象) 正确的对象，默认无
        """

        self.right_obj = right_obj

    def __str__(self):
        """
        异常描述信息
        :return info:(type=str) 异常描述
        """

        info = 'return或yield的对象类型错误！'
        if self.right_obj is not None:
            info += '正确对象的描述信息：%s' % str(self.right_obj)
        return info


class FaultReturn(BaseError):
    """
    继承重写方法后没有使用yield生成器的错误
    """

    def __str__(self):
        """
        异常描述信息
        :return info:(type=str) 异常描述
        """

        info = '应该使用yield而不是return！'
        return info


class ArgumentNumError(BaseError):
    """
    继承重写的方法接收传参数量不正确
    """

    def __str__(self):
        """
        异常描述信息
        :return info:(type=str) 异常描述
        """

        info = '接收传参数量不正确！'
        return info
