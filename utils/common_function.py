"""
非业务公共函数
"""

import sys
import time


def print_log(msg):
    """
    打印指定格式日志信息
    :param msg:(type=str) 日志信息
    """

    print('%s：%s' % (time.strftime('%Y-%m-%d %H:%M:%S'), msg))
    sys.stdout.flush()
