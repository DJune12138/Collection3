"""
项目：Collection3
业务所属公司：广州晶绮信息科技有限公司
开发者：耿东俊（June）
"""

from time import time
from importlib import import_module
from services.load import load
from utils import common_function as cf

if __name__ == '__main__':
    cf.print_log('服务启动！正在加载服务...')
    load()  # 加载所有服务
    cf.print_log('服务加载成功，引擎启动！')
    start = time()
    engine_module = import_module('framework.core.engine')
    engine = engine_module.Engine()  # 加载完所有服务后才加载引擎模块，否则所有服务依然是加载前的None
    engine.start()  # 引擎启动！
    cf.print_log('引擎关闭，总共耗时%s秒！' % (round(time() - start, 2)))
