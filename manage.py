"""
项目：数据采集通用框架
业务所属公司：广州晶绮信息科技有限公司
作者：耿东俊（June）
"""

import argparse
import importlib
from config import *
from utils.logger import Logger

# 获取脚本传参
parser = argparse.ArgumentParser(description='欢迎使用数据采集通用框架！详细使用方法请查看README。')
for parser_dict in p_parser.values():
    for parser_name, parser_setting in parser_dict.items():
        parser.add_argument('-' + parser_setting[pk_simple], '--' + parser_name, metavar=parser_name,
                            help=parser_setting[pk_help])
args = parser.parse_args()

# 校验传参
# 主参数，有且仅有一个
main_num = 0
main_list = p_parser[pk_main].keys()
main_key = ''
for parser_name in main_list:
    if getattr(args, parser_name.replace('-', '_')) is not None:
        main_num += 1
        if not main_key:
            main_key = parser_name
if main_num != 1:
    raise ValueError('主参数（%s）有且仅有一个！' % '、'.join(main_list))

# 导入自定义模块
args_name = getattr(args, main_key.replace('-', '_'))
try:
    m = importlib.import_module(
        '%s.%s.%s.%s' % (business_name, p_parser[pk_main][main_key][pk_module], args_name, args_name))
except ModuleNotFoundError:
    raise ModuleNotFoundError('%s为%s的模块不存在，请检查目录结构是否正确！' % (main_key, args_name))

# 获取日志器
lc = dict()
for k, v in logger_config.items():
    if v is not None:
        lc[k] = v
logger = Logger(**lc).logger

# 构建配置字典
config_dict = {
    'args': args,  # 脚本传参
    'logger': logger  # 日志器
}

# 运行模块
if __name__ == '__main__':
    obj = getattr(m, p_parser[pk_main][main_key][pk_gc])(config_dict)
    obj.run()
