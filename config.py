"""
1.项目配置参数
2.该文件中的配置都不允许随意删除，每个配置都有地方会引用
3.某部分配置有特定的数据结构，比如list、dict，或者更复杂的嵌套，请参照已配置好的格式进行修改
4.特别提醒，关于配置传参的隐含问题，如果需要--help打印出%，则需要%%
5.工厂配置在工厂模式使用，一般只提供给本config配置文件使用，各“工厂”一套配置
6.通用配置在各个地方都可以使用，包括业务
7.框架配置相一般是framework专用，同时也用于管理业务，比如控制开启业务、并发数等
"""

from factory import factory_code

"""工厂配置"""

factory_config = {
    1: {
        'logger_config.log_path': None  # 日志保存路径
    },
    2: {
        'logger_config.log_path': '/data/logs/c3_logs'
    }
}

"""通用配置"""

# 业务模块名称
business_name = 'business'

# 存放账号信息模块的名称
account_name = 'account'

# 脚本参数相关
pe_main = '该参数为主参数，至少有一个！'  # 主参数通用说明
pe_extra = '该参数为额外参数，可选，在某些业务可能是必选参数！'
pk_main = 'main'  # main的key
pk_gc = 'game-collection'
pk_simple = 'simple'
pk_help = 'help'
pk_module = 'module'
pk_builder = 'builder'
pk_pipeline = 'pipeline'
pk_builder_mw = 'builder_mw'
pk_downloader_mw = 'downloader_mw'
pk_ws = 'web-spider'
pk_extra = 'extra'
pk_st = 'start-time'
pk_et = 'end-time'
pk_ea = 'every-async'
p_parser = {  # 所有传参构成的字典
    pk_main: {  # 主参数
        pk_gc: {
            pk_simple: 'gc',  # 传参简写
            pk_help: '游戏采集，使用该参数即进入游戏日志数据采集。%s' % pe_main,  # 参数说明
            pk_module: 'game',  # 大类业务所属文件夹名
            pk_builder: 'GameBuilder',  # 大类业务建造器统一名称
            pk_pipeline: 'GamePipeline',  # 大类业务管道统一名称
            pk_builder_mw: 'GameBuilderMW',  # 大类业务建造器中间件统一名称
            pk_downloader_mw: 'GameDownloaderMW'  # 大类业务下载器中间件统一名称
        },
        pk_ws: {
            pk_simple: 'ws',
            pk_help: '网络爬虫，使用该参数即进入网络爬虫数据采集。%s' % pe_main,
            pk_module: 'spider',
            pk_builder: 'SpiderBuilder',
            pk_pipeline: 'SpiderPipeline',
            pk_builder_mw: 'SpiderBuilderMW',
            pk_downloader_mw: 'SpiderDownloaderMW'
        }
    },
    pk_extra: {  # 额外参数
        pk_ea: {
            pk_simple: 'a',
            pk_help: '每个业务开启的并发（并发倍数），必须为大于1的整数。'
        },
        pk_st: {
            pk_simple: 's',
            pk_help: '开始时间，格式为“%%Y%%m%%d”或“%%Y%%m%%d%%H%%M”。'  # %需要用%%
        },
        pk_et: {
            pk_simple: 'e',
            pk_help: '结束时间，格式为“%%Y%%m%%d”或“%%Y%%m%%d%%H%%M”。'
        }
    }
}

# 日志器配置
# 如使用默认参数，则为None
logger_config = {
    'level': None,
    'fmt': None,
    'date_fmt': None,
    'log_path': factory_config[factory_code]['logger_config.log_path'],
    'log_name': None
}

"""框架配置"""

# 最大并发数
# 必须为大于等于1的整数
# 请根据机器性能配置该数字，并不是越大越好
F_max_async = 50

# 每个业务开启的并发
F_every_async = 5
