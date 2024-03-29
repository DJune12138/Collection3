"""
1.项目配置参数
2.该文件中的配置都不允许随意删除，每个配置都有地方会引用
3.某部分配置有特定的数据结构，比如list、dict，或者更复杂的嵌套，请参照已配置好的格式进行修改
4.特别提醒，关于配置传参的隐含问题，如果需要--help打印出%，则需要%%
5.工厂配置在工厂模式使用，一般只提供给本config配置文件使用，各“工厂”一套配置
6.框架配置相一般是framework专用，同时也用于管理业务，比如控制开启业务、并发数等
7.通用配置在各个地方都可以使用，包括业务
"""

from factory import factory_code

"""工厂配置"""

factory_config = {
    1: {
        'logger_config.log_path': None,  # 日志保存路径
        'geoip2_path': r'D:\software\Python38\Lib\site-packages\_geoip_geolite2\GeoLite2-City.mmdb',  # geoip2驱动路径
        'ding_is_send': False,  # 是否发送钉钉
    },
    2: {
        'logger_config.log_path': '/data/logs/c3_logs',
        'geoip2_path': '/data/lib/geoipdb/GeoLite2-City.mmdb',
        'ding_is_send': True
    }
}

"""框架配置"""

# 最大并发数
# 必须为大于等于1的整数
# 请根据机器性能配置该数字，并不是越大越好
F_max_async = 12

# 每个业务开启的并发
F_every_async = 4

"""通用配置"""

# 业务模块名称
business_name = 'business'

# 存放账号信息模块的名称
account_name = 'account'

# 脚本参数相关
pe_main = '该参数为主参数，至少有一个！'  # 主参数通用说明
pe_extra = '该参数为额外参数，可选，在某些业务可能是必选参数！'
pk_main = 'main'  # main的key
pk_dt = 'demo-test'
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
pk_ps = 'pass-something'
pk_jd = 'just-do'
pk_type = 'type'
p_parser = {  # 所有传参构成的字典
    pk_main: {  # 主参数
        pk_dt: {
            pk_simple: 'dt',  # 传参简写
            pk_help: '演示和调试，使用该参数即进入架构调试。%s' % pe_main,  # 参数说明
            pk_module: 'demo',  # 大类业务所属文件夹名
            pk_builder: 'DemoBuilder',  # 大类业务建造器统一名称
            pk_pipeline: 'DemoPipeline',  # 大类业务管道统一名称
            pk_builder_mw: 'DemoBuilderMW',  # 大类业务建造器中间件统一名称
            pk_downloader_mw: 'DemoDownloaderMW'  # 大类业务下载器中间件统一名称
        },
        pk_gc: {
            pk_simple: 'gc',
            pk_help: '游戏采集，使用该参数即进入游戏日志数据采集。%s' % pe_main,
            pk_module: 'game',
            pk_builder: 'GameBuilder',
            pk_pipeline: 'GamePipeline',
            pk_builder_mw: 'GameBuilderMW',
            pk_downloader_mw: 'GameDownloaderMW'
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
        },
        pk_ps: {
            pk_simple: 'p',
            pk_help: '跳过某些步骤，不同业务不同用法，具体参照业务参数说明；在游戏数据采集流程中，如开启了生成旧报表的功能，则传参“t”可跳过生成旧报表。'
        },
        pk_jd: {
            pk_simple: 'j',
            pk_help: '只做某些步骤，不同业务不同用法，具体参照业务参数说明。'
        },
        pk_type: {
            pk_simple: 't',
            pk_help: '执行类型，不同业务不同用法，具体参照业务参数说明。'
        }
    }
}

# 日志器配置
# 如使用默认参数，则为None
logger_config = {
    'level': 'error',
    'fmt': None,
    'date_fmt': None,
    'log_path': factory_config[factory_code]['logger_config.log_path'],
    'log_name': None
}

# 钉钉机器人token
dk_plan = 'plan'  # plan的key
dk_live = 'live'
dk_exchange = 'exchange'
dk_marketing = 'marketing'
ding_token = {
    dk_plan: 'facaf0e73f3eb3d7536a0caed2c3001d073f33fc4fb3e9b8be8adfedcc6bc15d',  # 计划任务
    dk_live: 'fa66808a722dd7b2a147d47cc13a6d8e9db71b9d01ff2e613f2005c3cd30d881',  # 直播后台
    dk_exchange: '88ddb1e6c4e96c9ba76efd9bb064c1263eed928be354194525fce1dbbc3431b8',  # 汇率告警
    dk_marketing: 'e48c0350157ffd8fa8aa0f2da7687a1222953aa2263b992d579f04f86ff13de8'  # 行销告警
}
ding_is_send = factory_config[factory_code]['ding_is_send']

# 同类型（特征值相同）钉钉消息发送间隔（单位：秒）
ding_interval = 7200  # 2小时

# 游戏采集时间节点间隔（单位：分）
gc_interval = 10

# geoip2驱动路径
geoip2_path = factory_config[factory_code]['geoip2_path']

# 日期格式
format_date = '%Y%m%d'
format_datetime = '%Y%m%d%H%M'
format_date_n = '%Y-%m-%d'
format_datetime_n = '%Y-%m-%d %H:%M:%S'
