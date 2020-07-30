"""
引擎组件：
1.对外提供整个的程序的入口
2.依次调用其他组件对外提供的接口，实现整个框架的运作
"""

import time
import sys
import os
from importlib import import_module
from types import GeneratorType
from multiprocessing.dummy import Pool
from threading import Lock
from .builder import Builder
from .scheduler import Scheduler
from .downloader import Downloader
from .pipeline import Pipeline
from framework.object.request import Request
from framework.object.response import Response
from framework.object.item import Item
from framework.middlewares.builder_middlewares import BuilderMiddleware
from framework.middlewares.downloader_middlewares import DownloaderMiddleware
from framework.error.check_error import *
from utils import common_function as cf
from config import *
from services import logger, argv


class Engine(object):
    """
    引擎组件
    """

    def __init__(self):
        """
        初始配置
        """

        # 组件、中间件初始化
        try:
            self.__builders = dict()  # 建造器
            self.__scheduler = Scheduler()  # 调度器
            self.__downloader = Downloader()  # 下载器
            self.__pipelines = dict()  # 管道
            self.__builder_mws = dict()  # 建造器中间件
            self.__downloader_mws = dict()  # 下载器中间件
            self.__init_all()
            self.__builders_num = len(self.__builders)

            # 如果没开启任何业务，则可以直接关闭程序，防止引擎启动后卡死
            if self.__builders_num == 0:
                cf.print_log('没有获取到任何业务，引擎关闭！')
                sys.exit()

            # 请求与响应统计
            self.total_request_nums = 0
            self.total_response_nums = 0
            self.total_error_nums = 0

            # 异步任务相关
            self.__pool = Pool()  # 进程池
            self.__is_running = False  # 判断引擎是否可以关闭的标志
            self.request_mutex = Lock()  # 请求数互斥锁
            self.response_mutex = Lock()  # 响应数互斥锁
            self.error_mutex = Lock()  # 错误数互斥锁

            # 警告语模板
            total_warning = '，请规范写法，详情请查看报错提示信息、文档或相关类的说明。'
            self.__fr_warning = '业务建造器（{}）回调函数（{}）没有使用关键字yield%s' % total_warning
            self.__td_warning = '业务（{}）继承重写的方法没有返回正确的内置对象%s' % total_warning
            self.__ane_warning = '业务（{}）继承重写的方法没有接收正确数量的传参或参数名错误%s' % total_warning
            self.__cu_warning = '业务（{}）参数校验不通过%s' % total_warning
            self.__pe_warning = '业务（{}）下载器获取到的请求对象参数不正确%s' % total_warning
            self.__pue_warning = '业务（{}）组件里没有parse参数对应的解析函数%s' % total_warning
            self.__b_warning = '业务级错误！业务名称为{}。'
            self.__f_exception = '框架级错误！引擎原地爆炸！'

        # 初始化失败
        except Exception:
            logger.exception('框架级错误！引擎初始化失败！')
            sys.exit()

    def __init_all(self):
        """
        把通过脚本传参开启的业务初始化为引擎能用的格式
        """

        # 开始加载
        cf.print_log('开始加载所有业务模块...')

        # 默认对象
        # 默认对象是完全一样功能的对象，无需每循环一个就开辟一块内存，在循环前先创建
        pipeline = Pipeline()  # 默认管道
        builder_mw = BuilderMiddleware()  # 默认建造器中间件
        downloader_mw = DownloaderMiddleware()  # 默认下载器中间件

        # 1.校验并添加业务建造器
        # 业务建造器是整个业务最重要的组件，其中一步校验没通过都将跳过该业务
        # 主参数为*，会把对应模块下所有业务都开启
        # 注意，如果为*，引擎会把该模块下所有文件（夹）名称作为code，请不要把无关文件（夹）放在对应模块下
        for type_, codes in argv.main_key_dict.items():
            module_name = p_parser[pk_main][type_][pk_module]
            code_list = os.listdir('%s/%s' % (business_name, module_name)) if codes == '*' else codes.split(',')

            # 1.1 校验模块
            for code in set(code_list):
                cf.print_log('加载%s模块下的%s...' % (module_name, code))
                try:
                    m = import_module('%s.%s.%s.%s' % (business_name, module_name, code, code))
                except ModuleNotFoundError:
                    logger.exception('%s为%s的模块不存在，请检查目录结构是否正确！如使用*，请不要把无关文件（夹）放在对应模块下！' % (type_, code))
                    continue

                # 1.2 校验建造器规范
                try:
                    obj = getattr(m, p_parser[pk_main][type_][pk_builder])
                except AttributeError:
                    logger.exception('%s为%s的模块下没找到根据config配置的建造器类名%s，请规范写法。' % (
                        type_, code, p_parser[pk_main][type_][pk_builder]))
                    continue
                obj_name = obj.name  # 建造器名称（业务名称）
                if not issubclass(obj, Builder):  # 校验是否继承内置建造器
                    logger.exception('业务建造器（%s）没有继承内置建造器，请规范写法。' % obj_name)
                    continue

                # 1.3 校验业务名称唯一性
                if obj_name is None:
                    logger.exception('%s为%s的模块下的建造器对象%s没有name属性，name属性识别对应业务，必须有且是唯一值！' % (
                        type_, code, p_parser[pk_main][type_][pk_builder]))
                    continue
                elif obj_name in self.__builders.keys():
                    logger.exception('%s为%s的模块下的建造器对象%s的name属性（%s）与其他业务重复，name属性识别对应业务，必须有且是唯一值！' % (
                        type_, code, p_parser[pk_main][type_][pk_builder], obj_name))
                    continue
                elif obj_name in self.__builders.keys() or not isinstance(obj_name, str):
                    logger.exception('%s为%s的模块下的建造器对象%s的name属性不为字符串类型！请规范写法。' % (
                        type_, code, p_parser[pk_main][type_][pk_builder]))
                    continue

                # 1.4 全部校验通过，添加业务建造器
                try:
                    self.__builders[obj_name] = obj()
                except Exception:
                    logger.exception('业务建造器（%s）初始化失败！' % obj_name)
                    continue

                # 2.添加业务管道
                # 业务管道、中间件等都是附加组件，可以没有，没有则使用默认
                # 如果这些附加组件没有分别继承对应内置父类，也使用默认
                try:
                    obj = getattr(m, p_parser[pk_main][type_][pk_pipeline])
                except AttributeError:
                    self.__pipelines[obj_name] = pipeline
                else:
                    if issubclass(obj, Pipeline):
                        self.__pipelines[obj_name] = obj()
                    else:
                        self.__pipelines[obj_name] = pipeline
                        logger.exception('业务管道（%s）没有继承内置管道，已更换成默认管道！请规范写法。' % obj_name)

                # 3.添加业务建造器中间件
                try:
                    obj = getattr(m, p_parser[pk_main][type_][pk_builder_mw])
                except AttributeError:
                    self.__builder_mws[obj_name] = builder_mw
                else:
                    if issubclass(obj, BuilderMiddleware):
                        self.__builder_mws[obj_name] = obj()
                    else:
                        self.__builder_mws[obj_name] = builder_mw
                        logger.exception('业务建造器中间件（%s）没有继承内置建造器中间件，已更换成默认建造器中间件！请规范写法。')

                # 4.添加业务下载器中间件
                try:
                    obj = getattr(m, p_parser[pk_main][type_][pk_downloader_mw])
                except AttributeError:
                    self.__downloader_mws[obj_name] = downloader_mw
                else:
                    if issubclass(obj, DownloaderMiddleware):
                        self.__downloader_mws[obj_name] = obj()
                    else:
                        self.__downloader_mws[obj_name] = downloader_mw
                        logger.exception('业务下载器中间件（%s）没有继承内置下载器中间件，已更换成默认下载器中间件！请规范写法。')

        # 加载完成
        cf.print_log('所有业务模块加载完成！')

    def __call_back(self, temp):
        """
        异步线程池callback参数指向的函数，temp参数为固定写法
        :param temp:(type=?) 未知参数，底层模块写法，待探究
        """

        if self.__is_running:
            self.__pool.apply_async(self.__execute_request_response_item, callback=self.__call_back,
                                    error_callback=self.__error_callback)

    def __error_callback(self, exception):
        """
        异常回调函数，触发此函数的都是框架级错误
        :param exception:(type=Exception) 基于Exception类型的错误类
        """

        try:
            raise exception  # 抛出异常后，才能被日志进行完整记录下来
        except Exception:
            logger.exception(self.__f_exception)

    @staticmethod
    def __check_return(obj, right_obj=None):
        """
        用于校验继承重写返回对象是否正确，校验通过就返回对象，校验不通过则抛异常
        :param obj:(type=内置对象) 继承重写返回的对象
        :param right_obj:(type=内置对象) 正确的对象
        :return obj:(type=内置对象) 通过校验的对象
        """

        if right_obj is None:
            right_obj = GeneratorType
        if isinstance(obj, right_obj):
            return obj
        else:
            if right_obj == GeneratorType:  # yield问题返回yield相关异常
                raise FaultReturn
            else:
                raise TypeDifferent([right_obj])

    @staticmethod
    def __check_argument(func, *args):
        """
        用于校验继承重写函数接收参数数量是否正确，校验通过就返回执行结果，校验不通过则抛异常
        :param func:(type=function) 函数引用
        :param args:(type=tuple) 函数传参
        :return result:(type=∞) 函数执行结果
        """

        try:
            result = func(*args)
        except TypeError as e:
            if 'argument' in str(e):
                raise ArgumentNumError
            else:
                raise e
        else:
            return result

    @staticmethod
    def __check_parse(core, parse):
        """
        用于校验业务组件中解析函数的函数名是否正确，校验通过则返回函数引用，校验不通过则抛异常
        :param core:(type=业务组件) 业务组件对象
        :param parse:(type=str) 函数名
        :return func:(type=function) 函数引用
        """

        try:
            func = getattr(core, parse)
        except AttributeError:
            raise ParseUnExist
        else:
            return func

    def __statistics_lock(self, type_):
        """
        避免线性安全问题，利用互斥锁统计数据
        :param type_:(type=str) 数据的名称
        """

        lock = getattr(self, '%s_mutex' % type_)
        lock.acquire()
        try:
            nums_name = 'total_%s_nums' % type_
            setattr(self, nums_name, getattr(self, nums_name) + 1)
        except Exception as e:
            raise e
        finally:
            lock.release()

    def __add_request(self, request, builder_name):
        """
        添加请求任务
        :param request:(type=Request) 请求对象
        :param builder_name:(type=str) 业务名称
        """

        # 校验是否内置对象
        request = self.__check_return(request, right_obj=Request)

        # 建造器请求处理
        request = self.__check_return(self.__check_argument(self.__builder_mws[builder_name].process_request, request),
                                      right_obj=Request)

        # 给请求对象绑定建造器名称（业务名称）
        request.builder_name = builder_name

        # 把请求对象添加给调度器
        self.__scheduler.add_request(request)

        # 请求+1
        self.__statistics_lock('request')

    def __start_request(self):
        """
        处理初始请求
        """

        # 1.调用建造器，获取初始请求对象列表
        for builder_name, builder in self.__builders.items():
            try:
                start_list = self.__check_return(self.__check_argument(builder.start_requests))  # 校验是否yield
                empty = True  # 标记是否空start_list

                # 2.添加请求对象到调度器中
                for start_request in start_list:
                    empty = False
                    self.__add_request(start_request, builder_name)

                # 当编写人比较皮，所有业务建造器中的start都是一个空列表时，会出现引擎卡死的现象
                # 为了防止这种现象，如果start_list为空时，加个彩蛋请求，让引擎有机会关闭
                if empty:
                    self.__add_request(Request('test', parse='_funny'), builder_name)

            # 处理异常
            except FaultReturn:  # 校验yield不通过
                self.__add_request(Request('test', parse='_funny'), builder_name)  # 彩蛋请求，作用同上
                logger.exception(self.__fr_warning.format(builder_name, 'start_requests'))
            except TypeDifferent:  # 校验类型不通过
                self.__add_request(Request('test', parse='_funny'), builder_name)
                logger.exception(self.__td_warning.format(builder_name))
            except ArgumentNumError:  # 校验函数传参不通过
                self.__add_request(Request('test', parse='_funny'), builder_name)
                logger.exception(self.__ane_warning.format(builder_name))
            except CheckUnPass:  # start校验不通过
                self.__add_request(Request('test', parse='_funny'), builder_name)
                logger.exception(self.__cu_warning.format(builder_name))
            except Exception:  # 其他业务级错误
                self.__add_request(Request('test', parse='_funny'), builder_name)
                logger.exception(self.__b_warning.format(builder_name))

    def __execute_request_response_item(self):
        """
        处理后续请求与响应
        """

        # 3.调用调度器，获取请求对象
        # 如果获取请求对象的时候出错，就抛出框架级错误
        # 抛出错误后完成请求数+1（否则引擎会陷入死循环卡死而无法关闭），并直接结束该次任务
        try:
            request = self.__scheduler.get_request()
            if request is None:  # 如果没有获取到请求对象，直接结束
                return
            builder_name = request.builder_name  # 业务名称
            parse_name = request.parse  # 解析函数
        except Exception:
            self.__statistics_lock('error')
            self.__statistics_lock('response')
            logger.exception(self.__f_exception)
            return

        # 4.调用下载器，获取响应对象
        try:
            downloader_mw = self.__downloader_mws[builder_name]
            request = self.__check_return(self.__check_argument(
                downloader_mw.process_request, request), right_obj=Request)  # 下载器请求处理
            response = self.__downloader.get_response(request)
            response = self.__check_return(self.__check_argument(
                downloader_mw.process_response, response), right_obj=Response)  # 下载器响应处理
            response.meta = request.meta  # 信息（数据）互传

            # 5.调用建造器，解析响应对象
            response = self.__check_return(self.__check_argument(
                self.__builder_mws[builder_name].process_response, response), right_obj=Response)  # 建造器响应处理
            response_list = self.__check_return(
                self.__check_argument(self.__check_parse(self.__builders[builder_name], parse_name), response))

            # 6.根据响应对象类型，把该对象添加至调度器或交给管道
            for result in response_list:
                pipeline_result = None
                if isinstance(result, Request):
                    self.__add_request(result, builder_name)
                elif isinstance(result, Item):
                    pipeline_result = self.__check_argument(
                        self.__check_parse(self.__pipelines[builder_name], result.parse), result)
                else:
                    raise TypeDifferent([Request, Item])

                # 7.管道处理完数据对象后，根据处理结果返回的对象类型，添加请求对象至调度器或结束当次响应任务
                if isinstance(pipeline_result, Request):
                    self.__add_request(pipeline_result, builder_name)
                elif pipeline_result is None:
                    pass
                else:
                    raise TypeDifferent([Request, None])

        # 8.完成一个响应，响应+1
        # 无论是正常执行还是报错，都需要完成响应，否则引擎会一直卡死
        except FaultReturn:
            self.__statistics_lock('error')
            logger.exception(self.__fr_warning.format(builder_name, parse_name))
        except TypeDifferent:
            self.__statistics_lock('error')
            logger.exception(self.__td_warning.format(builder_name))
        except ArgumentNumError:
            self.__statistics_lock('error')
            logger.exception(self.__ane_warning.format(builder_name))
        except ParameterError:
            self.__statistics_lock('error')
            logger.exception(self.__pe_warning.format(builder_name))
        except ParseUnExist:
            self.__statistics_lock('error')
            logger.exception(self.__pue_warning.format(builder_name))
        except Exception:
            self.__statistics_lock('error')
            logger.exception(self.__b_warning.format(builder_name))
        finally:
            self.__statistics_lock('response')

    def __start_engine(self):
        """
        调用组件，框架运作
        """

        # 使用异步任务添加初始请求
        self.__is_running = True  # 启动引擎，设置状态为True
        self.__pool.apply_async(self.__start_request, error_callback=self.__error_callback)

        # 异步处理解析过程中产生的请求
        # 默认有多少个业务，就开启多少倍并发
        # 如果业务数大于控制的最大并发数，则使用配置的最大并发数
        # 可通过传参灵活控制并发倍数
        if not isinstance(F_max_async, int) or F_max_async < 1:
            raise CheckUnPass('配置config中的最大并发数F_max_async不为int类型或小于1，请修改！')
        if not isinstance(F_every_async, int) or F_every_async < 1:
            raise CheckUnPass('配置config中的并发倍数F_every_async不为int类型或小于1，请修改！')
        argv_ea = getattr(argv, pk_ea.replace('-', '_'))
        if argv_ea is not None:
            try:
                argv_ea = int(argv_ea)
            except ValueError:
                raise CheckUnPass('脚本传参%s（并发倍数）不为数字，请修改！' % pk_ea)
            if argv_ea < 1:
                raise CheckUnPass('脚本传参%s（并发倍数）小于1，请修改！' % pk_ea)
        every_async = argv_ea if argv_ea is not None else F_every_async
        async_count = self.__builders_num * every_async
        for i in range(async_count if async_count <= F_max_async else F_max_async):
            self.__pool.apply_async(self.__execute_request_response_item, callback=self.__call_back,
                                    error_callback=self.__error_callback)

        # 控制判断引擎关闭时机
        while True:
            time.sleep(0.001)  # 防止CPU空转
            if self.total_response_nums != 0:  # 由于异步任务，因此要响应不为0才开始判断
                if self.total_response_nums >= self.total_request_nums:
                    self.__is_running = False  # 标记可以关闭引擎
                    break

        # TODO 加一个流程，start_requests结束再来个end_requests
        pass

    def start(self):
        """
        启动引擎
        """

        try:
            self.__start_engine()
        except CheckUnPass as e:
            logger.exception(e)
            sys.exit()
        except Exception:
            logger.exception(self.__f_exception)
            sys.exit()
        cf.print_log('总共完成业务%s个！添加请求%s个，完成响应%s个，其中错误响应%s个！' % (
            self.__builders_num, self.total_request_nums, self.total_response_nums, self.total_error_nums))
