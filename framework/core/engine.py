"""
引擎组件：
1.对外提供整个的程序的入口
2.依次调用其他组件对外提供的接口，实现整个框架的运作
"""

from framework.object.request import Request
from .scheduler import Scheduler
from .downloader import Downloader
from .pipeline import Pipeline
from .builder import Builder


class Engine(object):
    """
    引擎组件
    """

    def __init__(self):
        """
        初始配置
        """

        self.builder = Builder()
        self.scheduler = Scheduler()
        self.downloader = Downloader()
        self.pipeline = Pipeline()

    def start(self):
        """
        启动引擎
        """

        self._start_engine()

    def _start_engine(self):
        """
        依次调用其他组件对外提供的接口，实现整个框架的运作
        """

        # 1.建造模块发出初始请求
        start_request = self.builder.start_requests()

        # 2.把初始请求添加给调度器
        self.scheduler.add_request(start_request)

        # 3.从调度器获取请求对象，交给下载器发起请求，获取一个响应对象
        request = self.scheduler.get_request()

        # 4.利用下载器发起请求
        response = self.downloader.get_response(request)

        # 5.利用建造器的解析响应的方法，处理响应，得到结果
        result = self.builder.parse(response)

        # 6.判断结果对象
        # 6.1 请求对象交给调度器处理
        if isinstance(result, Request):
            self.scheduler.add_request(result)

        # 6.2 数据对象交给管道处理
        else:
            self.pipeline.process_item(result)
