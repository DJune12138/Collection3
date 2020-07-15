"""
1.项目配置参数
2.该文件中的配置都不允许随意删除，每个配置都有地方会引用
3.某部分配置有特定的数据结构，比如list、dict，或者更复杂的嵌套，请参照已配置好的格式进行修改
4.框架配置相关参数是framework逻辑专用，同时也用于管理项目业务，比如控制开启业务、并发数等
"""

# 业务模块名称
business_name = 'business'

# 存放账号信息模块的名称
account_name = 'account'

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

# 日志器配置
# 如使用默认参数，则为None
logger_config = {
    'level': None,
    'fmt': None,
    'date_fmt': None,
    'log_path': None,
    'log_name': None
}

# 框架业务配置相关
# 最大并发数
# 必须为大于等于1的整数
# 请根据机器性能配置该数字，并不是越大越好
F_max_async = 10
