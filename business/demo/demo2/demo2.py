"""
演示和调试2
1.基于基础流程，演示自定义起始请求
2.演示在处理一个请求后，再发起另一个请求
3.演示请求与响应的数据通信
"""

from framework.core.builder import Builder


class DemoBuilder(Builder):
    name = 'demo2'

    def start_requests(self):
        """
        yield请求对象，请求对象带上meta参数，可实现数据通信
        """

        for i in range(3):
            yield self.request('test', meta={
                'msg': '我来自demo2的start_requests的第%s个请求' % (i + 1),
                'i': i
            })

    def parse(self, response):
        """
        yield请求对象，请求对象带上parse参数，可实现请求回调
        """

        print('我是demo2的parse，我接收到的响应信息是：%s' % response.meta['msg'])
        if response.meta['i'] == 2:
            yield self.request('test', parse='parse2', meta='我来自demo2的parse')
            print('我是demo2的parse，我yield了一个请求，让parse2处理这个请求得到的响应')
        else:
            item = self.item(None)
            yield item
            print('我是demo2的parse，我yield了一个数据%s' % item)

    def parse2(self, response):
        """
        该方法同样要接收一个响应对象
        """

        print('我是demo2的parse2，我接收到的响应信息是：%s' % response.meta)
        item = self.item(None)
        yield item
        print('我是demo2的parse2，我yield了一个数据%s' % item)
