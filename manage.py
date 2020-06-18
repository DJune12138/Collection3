"""
项目：数据采集通用框架
业务所属公司：广州晶绮信息科技有限公司
作者：耿东俊（June）
"""
import argparse
import importlib
import config

# 获取脚本传参
parser = argparse.ArgumentParser(description=config.description)
for parser_dict in config.all_parser.values():
    for parser_name, parser_setting in parser_dict.items():
        parser.add_argument('-' + parser_setting['simple'], '--' + parser_name, metavar=parser_name,
                            help=parser_setting['help'])
args = parser.parse_args()

# 校验传参
# 主参数，有且仅有一个
main_num = 0
main_list = config.all_parser['main'].keys()
main_key = ''
for parser_name in config.all_parser['main'].keys():
    if getattr(args, parser_name.replace('-', '_')) is not None:
        main_num += 1
        if not main_key:
            main_key = parser_name
if main_num != 1:
    raise ValueError('主参数（%s）有且仅有一个！' % '、'.join(main_list))

# 导入自定义模块
args_name = getattr(args, main_key.replace('-', '_'))
try:
    m = importlib.import_module('%s.%s.%s' % (config.all_parser['main'][main_key]['module'], args_name, args_name))
except ModuleNotFoundError:
    raise ModuleNotFoundError('%s为%s的模块不存在，请检查目录结构是否正确！' % (main_key, args_name))

# 运行模块
if __name__ == '__main__':
    obj = getattr(m, config.all_parser['main'][main_key]['object'])()
    obj.run()
