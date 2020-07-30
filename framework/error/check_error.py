"""
校验不通过的异常类
"""

from . import BaseError


class TypeDifferent(BaseError):
    """
    返回的对象类型错误，用于校验继承内置组件后返回的对象
    """

    def __init__(self, right_obj=None):
        """
        初始配置
        :param right_obj:(type=list) 正确的对象的列表，默认无
        """

        self.__right_obj = right_obj

    def __str__(self):
        """
        异常描述信息
        :return info:(type=str) 异常描述
        """

        info = 'return或yield的对象类型错误！'
        if self.__right_obj is not None:
            info += '正确对象的信息：%s' % '；'.join(
                ['%s.%s' % (i + 1, str(self.__right_obj[i])) for i, obj in enumerate(self.__right_obj)])
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

        info = '应该使用关键字yield（而不是return）一个对象！'
        return info


class ArgumentNumError(BaseError):
    """
    继承重写的方法接收传参（数量或参数名）不正确
    """

    def __str__(self):
        """
        异常描述信息
        :return info:(type=str) 异常描述
        """

        info = '接收传参（数量或参数名）不正确！'
        return info


class ParameterError(BaseError):
    """
    参数不正确，不为指定的参数
    """

    def __init__(self, parameter_name, right_parameter):
        """
        初始配置
        :param parameter_name:(type=str) 参数名
        :param right_parameter:(type=list) 正确的参数，元素为str类型
        """

        self.__parameter_name = parameter_name
        self.__right_parameter = right_parameter

    def __str__(self):
        """
        异常描述信息
        :return info:(type=str) 异常描述
        """

        info = '%s只能为%s中的其一！' % (self.__parameter_name, '、'.join(self.__right_parameter))
        return info


class CheckUnPass(BaseError):
    """
    其余的校验不通过
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

        info = self.__info if self.__info is not None else '校验不通过！'
        return info


class ParseUnExist(BaseError):
    """
    解析函数不存在对应的业务组件对象里的错误
    """

    def __str__(self):
        """
        异常描述信息
        :return info:(type=str) 异常描述
        """

        info = '解析函数不存在对应的业务组件对象里！'
        return info


class LackParameter(BaseError):
    """
    缺少参数
    """

    def __init__(self, parameter):
        """
        初始配置
        :param parameter:(type=list) 缺少的参数名，元素为str类型
        """

        self.__parameter = parameter

    def __str__(self):
        """
        异常描述信息
        :return info:(type=str) 异常描述
        """

        info = '缺少必要的参数（%s）！' % '、'.join(self.__parameter)
        return info
