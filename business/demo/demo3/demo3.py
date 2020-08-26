"""
演示和调试3
1.基于基础流程，演示自定义管道
2.演示自定义管道再发起另外一个请求，并指定建造器函数
3.演示指定解析数据对象的管道函数
"""

from framework.core.builder import Builder
from framework.core.pipeline import Pipeline  # 同建造器，必须导入框架的管道用于继承


class DemoBuilder(Builder):
    name = 'demo3'
    start = [{'way': 'test'}]

    def parse(self, response):
        print('我是demo3的parse，我接收了一个参数%s' % response)
        item = self.item(None)
        yield item
        print('我是demo3的parse，我yield了一个数据%s' % item)

    def parse2(self, response):
        """
        构造数据对象时可以指定parse参数，业务管道将会用该函数处理这个数据
        """

        print('我是demo3的parse2，我接收到的响应信息是：%s' % response.meta)
        item = self.item('我来自demo3的parse2', parse='process_item2')
        yield item
        print('我是demo3的parse2，我yield了一个数据，让process_item2处理这个数据')


class DemoPipeline(Pipeline):
    """
    类名需与config里的一致
    """

    def process_item(self, item):
        """
        这里可以编写解析数据的业务逻辑，需接收一个参数，为数据对象，可以yield请求对象
        """

        print('我是demo3的process_item，我接收了一个参数%s' % item)
        for i in range(3):
            yield self.request('test', parse='parse2', meta='我来自demo3的process_item的第%s个请求' % (i + 1))

    def process_item2(self, item):
        """
        该方法同样要接收一个数据对象
        """

        print('我是demo3的process_item2，我接收到的数据信息是：%s' % item.data)
