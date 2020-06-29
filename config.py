"""
项目配置
"""

# 业务模块名称
business_name = 'business'

# 脚本参数相关
pe_main = '该参数为主参数，有且仅有一个！'  # 主参数通用说明
pe_extra = '该参数为额外参数，可选，在某些业务可能是必选参数！'
pk_main = 'main'  # main的key
pk_gc = 'game-code'
pk_simple = 'simple'
pk_help = 'help'
pk_module = 'module'
pk_builder = 'builder'
pk_pipeline = 'pipeline'
pk_builder_mw = 'builder_mw'
pk_downloader_mw = 'downloader_mw'
p_parser = {  # 所有传参构成的字典
    pk_main: {  # 主参数
        pk_gc: {
            pk_simple: 'gc',  # 传参简写
            pk_help: '游戏代码，使用该参数即进入游戏日志采集。%s' % pe_main,  # 参数说明
            pk_module: 'game',  # 大类业务所属文件夹名
            pk_builder: 'GameBuilder',  # 大类业务建造器统一名称
            pk_pipeline: 'GamePipeline',  # 大类业务管道统一名称
            pk_builder_mw: 'GameBuilderMW',  # 大类业务建造器中间件统一名称
            pk_downloader_mw: 'GameDownloaderMW'  # 大类业务下载器中间件统一名称
        }
    },
    'extra': {}  # 额外参数
}

# 日志器配置（如使用默认参数，则为None）
logger_config = {
    'level': None,
    'fmt': None,
    'date_fmt': None,
    'log_path': None,
    'log_name': None
}

"""以下和框架配置相关"""
# 要开启的业务
F_builders = {
    pk_gc: []
}
