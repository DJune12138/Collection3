"""
项目配置
"""

# 版本号（随时更新）
version = 'V0.1'

# 脚本参数相关
description = '欢迎使用数据采集通用框架（版本号：%s）！详细使用方法请查看README。' % version  # help
main_explain = '该参数为主参数，有且仅有一个！'  # 主参数通用说明
extra_explain = '该参数为额外参数，可选，在某些业务可能是必选参数！'  # 额外参数通用说明
all_parser = {
    'main': {  # 主参数
        'game-code': {
            'simple': 'gc',
            'help': '游戏代码，使用该参数即进入游戏日志采集。%s' % main_explain,
            'module': 'game',
            'object': 'GameData'
        }
    },
    'extra': {}  # 额外参数
}

# 业务模块名称
business_name = 'business'
