"""
项目配置
"""

# 版本号（随时更新）
version = 'V0.1'

# 业务模块名称
business_name = 'business'

# 脚本参数相关
pe_main = '该参数为主参数，有且仅有一个！'  # 主参数通用说明
pe_extra = '该参数为额外参数，可选，在某些业务可能是必选参数！'
pk_main = 'main'  # main的key
pk_simple = 'simple'
pk_help = 'help'
pk_module = 'module'
pk_object = 'object'
p_parser = {  # 所有传参构成的字典
    pk_main: {  # 主参数
        'game-code': {
            pk_simple: 'gc',
            pk_help: '游戏代码，使用该参数即进入游戏日志采集。%s' % pe_main,
            pk_module: 'game',
            pk_object: 'GameData'
        }
    },
    'extra': {}  # 额外参数
}

# 日志器相关（如使用默认参数，则为None）
logger_config = {
    'level': None,
    'fmt': None,
    'date_fmt': None,
    'log_path': None,
    'log_name': None
}
