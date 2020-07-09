"""
项目：Collection3
业务所属公司：广州晶绮信息科技有限公司
开发者：耿东俊（June）
"""

import time
from importlib import reload
from framework.core import engine
from services.load import load
from utils import common_function as cf

if __name__ == '__main__':
    cf.print_log('服务启动！正在加载服务...')
    load()  # 加载所有服务
    reload(engine)  # 加载完所有服务需要重载引擎模块，否则所有服务依然是加载前的None
    cf.print_log('服务加载成功，引擎启动！')
    start = time.time()
    engine = engine.Engine()
    engine.start()
    cf.print_log('引擎关闭，总共耗时%s秒！' % (round(time.time() - start, 2)))
