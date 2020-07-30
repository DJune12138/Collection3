"""
管道组件：
1.负责处理数据对象
"""

from framework.object.request import Request
from framework.error.check_error import ParameterError
from services import mysql


class Pipeline(object):
    """
    管道组件
    """

    def _funny(self, item):
        """
        彩蛋流程专用，这个函数一般只用于彩蛋，不用于继承重写或参与业务
        """

        pass

    def process_item(self, item):
        """
        1.处理数据对象
        2.继承后，可重写该方法，自行编写获取到数据后的业务逻辑
        3.重写该方法必须要接收一个参数（无论业务使用与否），是数据对象
        4.重写该方法后可返回一个请求对象，让引擎继续把请求交给调度器
        5.该方法默认把解析后的数据直接插入数据库
        :param item:(type=Item) 建造器通过引擎交过来的数据对象
        :return result:(type=Request,None) 处理完该数据对象后返回的结果，返回None或什么都不返回则意味着完成当前一条流程
        """

        # 把获取到的数据入库
        data = item.data
        if data is not None:
            self.into_db(data)

        # 结束流程
        result = None
        return result

    @staticmethod
    def request(*args, **kwargs):
        """
        为业务管道提供生成内置请求对象的接口
        :param args:(type=tuple) 生成请求对象的请求信息，可变参数
        :param kwargs:(type=dict) 生成请求对象的请求信息，关键词参数
        :return request:(type=Request) 内置请求对象
        """

        request = Request(*args, **kwargs)
        return request

    @staticmethod
    def into_db(data):
        """
        入库的通用接口
        :param item:(type=dict) 解析后，准备入库的数据；db_type指定数据库类型，db_name指定关键字映射的数据库对象，其余参数参考工具包
        """

        db_type = data.setdefault('db_type', 'mysql')
        db_name = data.setdefault('db_name', '')
        del data['db_type']
        del data['db_name']
        if db_type == 'mysql':
            mysql_db = mysql[db_name]
            mysql_db.insert(**data)
        else:
            raise ParameterError('db_type', ['mysql'])
