"""
引擎组件：
1.对外提供整个的程序的入口
2.依次调用其他组件对外提供的接口，实现整个框架的运作
"""

import time
from importlib import import_module
from .scheduler import Scheduler
from .downloader import Downloader
from .pipeline import Pipeline
from framework.object.request import Request
from framework.middlewares.builder_middlewares import BuilderMiddleware
from framework.middlewares.downloader_middlewares import DownloaderMiddleware
from utils import common_function as cf
from config import *


class Engine(object):
    """
    引擎组件
    """

    def __init__(self):
        """
        初始配置
        """

        # 初始化开始
        cf.print_log('引擎初始化...')

        # 组件、中间件初始化
        self.builders = dict()  # 建造器
        self.scheduler = Scheduler()  # 调度器
        self.downloader = Downloader()  # 下载器
        self.pipelines = dict()  # 管道
        self.builder_mws = dict()  # 建造器中间件
        self.downloader_mws = dict()  # 下载器中间件
        self._init_all()

        # 请求与响应统计
        self.total_request_nums = 0
        self.total_response_nums = 0

    def _init_all(self):
        """
        把在config配置的业务初始化为引擎能用的格式
        """

        # 默认对象
        # 默认对象是完全一样功能的对象，无需每循环一个就开辟一个内存，在循环前先创建
        pipeline = Pipeline()  # 默认管道
        builder_mw = BuilderMiddleware()  # 默认建造器中间件
        downloader_mw = DownloaderMiddleware()  # 默认下载器中间件

        # 1.校验并添加业务建造器，必须要有
        for type_, code_list in F_builders.items():
            for code in code_list:

                # 1.1 校验模块
                try:
                    m = import_module('%s.%s.%s.%s' % (business_name, p_parser[pk_main][type_][pk_module], code, code))
                except ModuleNotFoundError:
                    raise ModuleNotFoundError('%s为%s的模块不存在，请检查目录结构是否正确！' % (type_, code))

                # 1.2 校验建造器
                try:
                    obj = getattr(m, p_parser[pk_main][type_][pk_builder])
                except AttributeError:
                    raise AttributeError('%s为%s的模块下没有建造器对象（%s），建造器对象必须有！'
                                         % (type_, code, p_parser[pk_main][type_][pk_builder]))

                # 1.3 校验业务名称唯一性
                obj_name = obj.name  # 建造器名称（业务名称）
                if self.builders.get(obj_name) is not None or not obj_name or not isinstance(obj_name, str):
                    raise ValueError('%s为%s的模块下的建造器对象的name属性（%s）与其他业务重复或不为字符串类型，name属性识别对应业务，必须是唯一值！'
                                     % (type_, code, obj_name))

                # 1.4 添加业务建造器
                self.builders[obj_name] = obj()

                # 2.添加业务管道，没有则使用默认
                try:
                    obj = getattr(m, p_parser[pk_main][type_][pk_pipeline])
                except AttributeError:
                    self.pipelines[obj_name] = pipeline
                else:
                    self.pipelines[obj_name] = obj()

                # 3.添加业务建造器中间件，没有则使用默认
                try:
                    obj = getattr(m, p_parser[pk_main][type_][pk_builder_mw])
                except AttributeError:
                    self.builder_mws[obj_name] = builder_mw
                else:
                    self.builder_mws[obj_name] = obj()

                # 4.添加业务下载器中间件，没有则使用默认
                try:
                    obj = getattr(m, p_parser[pk_main][type_][pk_downloader_mw])
                except AttributeError:
                    self.downloader_mws[obj_name] = downloader_mw
                else:
                    self.downloader_mws[obj_name] = obj()

    def start(self):
        """
        启动引擎
        """

        start = time.time()
        cf.print_log('引擎初始化完成，启动！')
        self._start_engine()
        cf.print_log('引擎关闭！')
        cf.print_log('耗时%s秒！请求%s个，完成响应%s个！' % (round(time.time() - start, 2), self.total_request_nums,
                                               self.total_response_nums))

    def _start_request(self):
        """
        处理初始请求
        """

        # 1.调用建造器，获取初始请求对象列表
        for builder_name, builder in self.builders.items():
            start_list = builder.start_requests()

            # 2.添加请求对象到调度器中
            for start_request in start_list:
                start_request = self.builder_mws[builder_name].process_request(start_request)  # 建造器请求处理
                start_request.builder_name = builder_name  # 给请求对象绑定建造器名称（业务名称）
                self.scheduler.add_request(start_request)
                self.total_request_nums += 1  # 请求+1

    def _execute_request_response_item(self):
        """
        处理后续请求与响应
        """

        # 3.调用调度器，获取请求对象
        request = self.scheduler.get_request()
        if request is None:  # 如果没有获取到请求对象，直接结束
            return
        builder_name = request.builder_name  # 取出业务名称

        # 4.调用下载器，获取响应对象
        downloader_mw = self.downloader_mws[builder_name]
        request = downloader_mw.process_request(request)  # 下载器请求处理
        response = self.downloader.get_response(request)
        response = downloader_mw.process_response(response)  # 下载器响应处理
        response.meta = request.meta  # 信息（数据）互传

        # 5.调用建造器，解析响应对象
        builder_mw = self.builder_mws[builder_name]
        response = builder_mw.process_response(response)  # 建造器响应处理
        parse = getattr(self.builders[builder_name], request.parse)  # 指定解析函数
        response_list = parse(response)

        # 6.根据响应对象类型，把该对象添加至调度器或交给管道
        for result in response_list:
            if isinstance(result, Request):
                result = builder_mw.process_request(result)  # 建造器请求处理
                result.builder_name = builder_name
                self.scheduler.add_request(result)
                self.total_request_nums += 1  # 请求+1
            else:
                self.pipelines[builder_name].process_item(result)

        # 7.完成一个响应，响应+1
        self.total_response_nums += 1

    def _start_engine(self):
        """
        依次调用组件，框架运作
        """

        self._start_request()
        while True:
            time.sleep(0.001)  # 防止CPU空转
            self._execute_request_response_item()
            if self.total_response_nums >= self.total_request_nums:  # 完成响应任务，结束程序
                break
