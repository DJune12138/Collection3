"""
管道组件：
1.负责处理数据对象
"""

from framework.object.request import Request
from framework.error.check_error import ParameterError
from services import mysql, redis, clickhouse
from utils import common_profession as cp


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
            else:
                data['ignore'] = True
            if detail == 'online':  # 在线
                data['table'] = 'oper_game_online'
                data['columns'] = ['gamecode', 'servercode', 'online_time', 'online_count']
                data['values'] = [game_code, server_code, time, source['count']]
            elif detail in ('register', 'login', 'pay'):  # 注册、登录、储值
                userid = source['userid']
                puid = source.get('puid') if source.get('puid') else userid
                ip = source.get('ip', '')
                area = cp.ip_belong(ip)['code']
                area_code = 'TW' if area == '' else area
                os = 'IOS' if source.get('os', '').lower() == 'ios' else 'Android'
                if detail == 'register':  # 注册
                    data['table'] = 'oper_game_user'
                    data['columns'] = ['gamecode', 'servercode', 'regdate', 'userid', 'puid',
                                       'ip', 'os', 'areacode', 'regtime']
                    data['values'] = [game_code, server_code, time[:10], userid, puid,
                                      ip, os, area_code, time]
                elif detail == 'login':  # 登录
                    data['table'] = 'oper_game_login'
                    data['columns'] = ['gamecode', 'servercode', 'logindate', 'userid', 'puid',
                                       'ip', 'os', 'areacode', 'logintime']
                    data['values'] = [game_code, server_code, time[:10], userid, puid,
                                      ip, os, area_code, time]
                elif detail == 'pay':  # 储值
                    data['table'] = 'oper_game_pay'
                    data['columns'] = ['gamecode', 'orderid', 'servercode', 'paydate', 'userid',
                                       'puid', 'ip', 'os', 'areacode', 'paytime',
                                       'amt']
                    data['values'] = [game_code, source['order_id'], server_code, time[:10], userid,
                                      puid, ip, os, area_code, time,
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
            mysql_db.insert(**data)
        elif db_type == 'redis':
            redis_db = redis[db_name]
            getattr(redis_db, data.get('redis_set', 'set'))(**data)
        elif db_type == 'clickhouse':
            clickhouse_db = clickhouse[db_name]
            clickhouse_db.insert(**data)
        else:
            raise ParameterError('db_type', ['mysql', 'redis', 'clickhouse'])
