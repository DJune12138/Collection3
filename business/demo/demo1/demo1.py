"""
演示和调试1
1.演示基础流程
"""

from framework.core.builder import Builder  # 导入框架的建造器组件并用于继承，必须


class DemoBuilder(Builder):
    """
    类名需与config里的一致
    """

    name = 'demo1'  # 业务名称，必须有且需要与其他业务名称不重复
    start = [{'way': 'test'}]  # 起始请求，way为必须

    def parse(self, response):
        """
        这里可以编写解析响应的业务逻辑，需接收一个参数，为响应对象，并且yield数据对象
        """

        print('我是demo1的parse，我接收了一个参数%s' % response)
        item = self.item(None)
        yield item
        print('我是demo1的parse，我yield了一个数据%s' % item)
