import argparse
import json
from concurrent.futures import ThreadPoolExecutor
import services
from config import *
from utils.logger import Logger
from utils.mysql import MySQL


def __connect_mysql(name, info):
    """
    用于多任务创建连接池
    :param name:(type=str) 连接池的key
    :param info:(type=dict) 连接的信息
    """

    host = info.get('host', 'localhost')
    port = info.get('port', 3306)
    try:
        services.mysql[name] = MySQL(host=host, port=port, user=info['user'], password=info['password'], db=info['db'],
                                     charset=info.get('charset', 'utf8'))
    except Exception:
        services.logger.exception('MySQL数据库（host为%s，port为%s）连接失败！' % (host, port))


def __get_argv():
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
        argv = parser.parse_args()
        argv.main_key_dict = dict()  # 引擎用，用于记录开启哪一块的业务

        # 2.校验脚本传参
        # 主参数，至少有一个
        main_list = p_parser[pk_main].keys()
        for parser_name in main_list:
            codes = getattr(argv, parser_name.replace('-', '_'))
            if codes is not None:
                argv.main_key_dict[parser_name] = codes
        if len(argv.main_key_dict) < 1:
            raise ValueError('主参数（%s）至少有一个！' % '、'.join(main_list))

        # 3.校验通过，加载公共脚本传参
        services.argv = argv


def __get_logger():
    """
    根据配置，加载公共日志器
    """

    if services.logger is None:
        lc = dict()
        for k, v in logger_config.items():
            if v is not None:
                lc[k] = v
        services.logger = Logger(**lc).logger


def __get_mysql():
    """
    加载MySQL数据库连接池
    """

    if services.mysql is None:
        services.mysql = dict()
        with open('%s/mysql.json' % account_name, 'r') as f:
            mysql_account = json.load(f)
        with ThreadPoolExecutor() as executor:  # 用异步任务去连接数据库，每个任务相互独立，互不影响
            for name, info in mysql_account.items():
                executor.submit(__connect_mysql, name, info)


def load():
    """
    加载所有服务
    """

    __get_argv()
    __get_logger()
    __get_mysql()
