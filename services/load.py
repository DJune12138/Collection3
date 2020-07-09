import argparse
import services
from config import *
from utils.logger import Logger


def get_argv():
    """
    检验并加载公共脚本传参
    """

    # 1.获取脚本传参
    if services.argv is None:  # 防止加载一次后再次加载
        parser = argparse.ArgumentParser(description='欢迎使用Collection3！详细使用方法请查看README。')
        for parser_dict in p_parser.values():
            for parser_name, parser_setting in parser_dict.items():
                parser.add_argument('-' + parser_setting[pk_simple], '--' + parser_name, metavar=parser_name,
                                    help=parser_setting[pk_help])
        args = parser.parse_args()

        # 2.校验脚本传参
        # 主参数，至少有一个
        main_list = p_parser[pk_main].keys()
        for parser_name in main_list:
            codes = getattr(args, parser_name.replace('-', '_'))
            if codes is not None:
                services.main_key[parser_name] = codes
        if len(services.main_key) < 1:
            raise ValueError('主参数（%s）至少有一个！' % '、'.join(main_list))

        # 3.校验通过，加载公共脚本传参
        services.argv = args


def get_logger():
    """
    根据配置，加载公共日志器
    """

    if services.logger is None:
        lc = dict()
        for k, v in logger_config.items():
            if v is not None:
                lc[k] = v
        services.logger = Logger(**lc).logger


def load():
    """
    加载所有服务
    """

    get_argv()
    get_logger()
