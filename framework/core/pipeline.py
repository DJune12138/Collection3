"""
管道组件：
1.负责处理数据对象
"""

from framework.object.request import Request
from framework.error.check_error import ParameterError
from services import mysql
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
            if detail == 'online':  # 在线数
                data['table'] = 'oper_game_online'
                data['values'] = [source['gamecode'], source['servercode'], source['online_time'],
                                  source['online_count']]
                data['columns'] = ['gamecode', 'servercode', 'online_time', 'online_count']
                data['duplicates'] = ['online_count']
            elif detail in ('register', 'login', 'pay'):  # 注册、登录、储值
                time = source['time']
                userid = source['userid']
                puid = source.get('puid') if source.get('puid') else userid
                ip = source.get('ip', '')
                os = source.get('os', 'unknow')
                if os.lower() == 'android':
                    os = 'Android'
                elif os.lower() == 'ios':
                    os = 'IOS'
                area_code = cp.ip_belong(ip)['code']
                data['ignore'] = True
                if detail == 'register':
                    data['table'] = 'oper_game_user'
                    data['columns'] = ['gamecode', 'servercode', 'regdate', 'userid', 'puid',
                                       'ip', 'os', 'areacode', 'regtime']
                    data['values'] = [source['gamecode'], source['servercode'], time[:10], userid, puid,
                                      ip, os, area_code, time]
                elif detail == 'login':
                    data['table'] = 'oper_game_login'
                    data['columns'] = ['gamecode', 'servercode', 'logindate', 'userid', 'puid',
                                       'ip', 'os', 'areacode', 'logintime']
                    data['values'] = [source['gamecode'], source['servercode'], time[:10], userid, puid,
                                      ip, os, area_code, time]
                else:
                    data['table'] = 'oper_game_pay'
                    data['columns'] = ['gamecode', 'orderid', 'servercode', 'paydate', 'userid',
                                       'puid', 'ip', 'os', 'areacode', 'paytime',
                                       'amt']
                    data['values'] = [source['gamecode'], source['order_id'], source['servercode'], time[:10], userid,
                                      puid, ip, os, area_code, time,
                                      source['amt']]
        if data is not None:
            self.into_db(data)

        # 结束流程
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
        else:
            raise ParameterError('db_type', ['mysql'])
