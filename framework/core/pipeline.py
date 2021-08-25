"""
管道组件：
1.负责处理数据对象
"""

from framework.object.request import Request
from framework.error.check_error import ParameterError
from services import mysql, redis, clickhouse, logger
from utils import common_profession as cp
from utils import common_function as cf
from utils.mysql import ExecuteError as mysql_exe
from utils.clickhouse import ExecuteError as clickhouse_exe


class Pipeline(object):
    """
    管道组件
    """

    def _funny(self, item):
        """
        彩蛋流程专用，这个函数一般只用于彩蛋，不用于继承重写或参与业务
        """

        pass

    def process_item(self, item):
        """
        1.处理数据对象
        2.继承后，可重写该方法，自行编写获取到数据后的业务逻辑
        3.重写该方法必须要接收一个参数（无论业务使用与否），是数据对象
        4.重写该方法后可返回一个请求对象，让引擎继续把请求交给调度器
        5.该方法默认把解析后的数据直接插入数据库，当detail属性不为None时则启用入库明细功能
        6.platform为osa平台，jq（晶绮）、cx（初心）、hy（和悦）
        7.source(type=list)为解析后的源数据，函数会自动获取对应键值入库对应字段
        :param item:(type=Item) 建造器通过引擎交过来的数据对象
        :return result:(type=Request,None) 处理完该数据对象后返回的结果，返回None或什么都不返回则意味着完成当前一条流程
        """

        # 把获取到的数据入库
        # 默认根据data普通入库
        # 如detail不为None，则代表启用入库明细功能，data会加点戏
        detail = item.detail
        data = item.data
        if detail is not None:
            source = data['source']
            data['db_name'] = 'osa_' + data['platform']
            game_code = source['gamecode']
            server_code = source['servercode']
            time = source['time']
            duplicates = source.get('duplicates')
            if duplicates:
                data['duplicates'] = duplicates
                if source.get('dup_ac'):
                    data['dup_ac'] = True
            else:
                data['ignore'] = True
            if detail == 'online':  # 在线
                data['table'] = 'oper_game_online'
                data['columns'] = ['gamecode', 'servercode', 'online_time', 'online_count']
                data['values'] = [game_code, server_code, time, source['count']]
            elif detail in ('register', 'login', 'pay'):  # 注册、登录、储值
                user_id = source['userid']
                p_uid = source.get('puid') if source.get('puid') else user_id
                ip = source.get('ip')
                ip = ip if ip is not None else ''  # 未知情况下IP传入时就是为None，防止后续字符串拼接报错，当为None时为空字符串
                area = cp.ip_belong(ip)['code']
                area_code = 'TW' if area == '' else area
                source_os = source.get('os', '')
                if detail == 'pay':
                    os = 'IOS' if 'ios' in source_os.lower() else 'Android'
                else:
                    if 'ios' in source_os.lower():
                        os = 'IOS'
                    elif 'android' in source_os.lower() or not source_os:
                        os = 'Android'
                    else:
                        os = source_os
                if detail == 'register':  # 注册
                    data['table'] = 'oper_game_user'
                    data['columns'] = ['gamecode', 'servercode', 'regdate', 'userid', 'puid',
                                       'ip', 'os', 'areacode', 'regtime']
                    data['values'] = [game_code, server_code, time[:10], user_id, p_uid,
                                      ip, os, area_code, time]
                elif detail == 'login':  # 登录
                    data['table'] = 'oper_game_login'
                    data['columns'] = ['gamecode', 'servercode', 'logindate', 'userid', 'puid',
                                       'ip', 'os', 'areacode', 'logintime']
                    data['values'] = [game_code, server_code, time[:10], user_id, p_uid,
                                      ip, os, area_code, time]
                elif detail == 'pay':  # 储值
                    data['table'] = 'oper_game_pay'
                    data['columns'] = ['gamecode', 'orderid', 'servercode', 'paydate', 'userid',
                                       'puid', 'ip', 'os', 'areacode', 'paytime',
                                       'amt']
                    data['values'] = [game_code, source['order_id'], server_code, time[:10], user_id,
                                      p_uid, ip, os, area_code, time,
                                      source['amt']]
        if data is not None:
            self.into_db(data)

        # 返回None，结束流程
        result = None
        return result

    @staticmethod
    def request(*args, **kwargs):
        """
        为业务管道提供生成内置请求对象的接口
        :param args:(type=tuple) 生成请求对象的请求信息，可变参数
        :param kwargs:(type=dict) 生成请求对象的请求信息，关键字参数
        :return request:(type=Request) 内置请求对象
        """

        request = Request(*args, **kwargs)
        return request

    @staticmethod
    def into_db(data):
        """
        入库的通用接口
        1.db_type指定数据库类型
        2.db_name指定关键字映射的数据库对象
        3.其余参数参考工具包
        :param data:(type=dict) 解析后，准备入库的数据
        """

        db_type = data.get('db_type', 'mysql')
        db_name = data.get('db_name')
        if db_type == 'mysql':
            mysql_db = mysql[db_name]
            try:
                mysql_db.insert(**data)
            except mysql_exe as e:
                if 'Lock wait timeout exceeded' in str(e):  # 多线程引发的唯一键冲突，有一定概率发生，暂没有好办法避免
                    cf.print_log('多线程下概率引发的唯一键冲突！')
                else:
                    raise e
        elif db_type == 'redis':
            redis_db = redis[db_name]
            getattr(redis_db, data.get('redis_set', 'set'))(**data)
        elif db_type == 'clickhouse':
            clickhouse_db = clickhouse[db_name]
            try:
                clickhouse_db.insert(**data)
            except clickhouse_exe as e:
                if 'most likely due to a circular import' in str(e):  # clickhouse_driver源代码未知错误，暂无法解决
                    cf.print_log('clickhouse_driver源代码未知错误！')  # 预估问题和clickhouse的ReplacingMergeTree引擎有关
                else:
                    raise e
        else:
            raise ParameterError('db_type', ['mysql', 'redis', 'clickhouse'])
