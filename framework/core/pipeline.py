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
        处理item对象
        :param item:(type=item) 建造器交过来的数据对象
        """

        print('item: ', item)
