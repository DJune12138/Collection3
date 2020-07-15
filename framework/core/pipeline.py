"""
管道组件：
1.负责处理数据对象
"""


class Pipeline(object):
    """
    管道组件
    """

    def process_item(self, item):
        """
        1.处理数据对象
        2.继承后，可重写该方法，自行编写获取到数据后的业务逻辑
        3.重写该方法必须要接收一个参数（无论业务使用与否），是数据对象
        :param item:(type=item) 建造器通过引擎交过来的数据对象
        """

        pass
