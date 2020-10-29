"""
建造器组件：
1.构建请求信息，生成请求对象
2.解析响应对象，返回数据对象或者新的请求对象
"""

from framework.object.request import Request
from framework.object.item import Item
from framework.error.check_error import CheckUnPass
from utils import common_profession as cp, common_function as cf
from services import launch


class Builder(object):
    """
    建造器组件
    """

    # 建造器名称
    # 继承类后重写该属性即可，必须重写
    # 用于识别业务，每个业务应为唯一名称，必须使用字符串
    name = None

    # 默认初始请求
    # 继承类后重写该属性即可，必须为list类型
    # 目前只有start_requests方法会调用该属性，如继承类后重写了start_requests方法，可以不需要该属性
    # 元素必须为dict类型，且必须有“way”这对键值
    # 原生start_requests函数会把列表中每一个mapping作为关键字参数构建请求对象
    start = [{'way': 'test', 'parse': '_funny'}]

    # 是否使用通用的游戏数据采集流程
    # 继承类后可重写该属性，True为使用，False为不使用，不重写则默认不使用
    # 如果使用通用流程，则不再进入start_requests函数，而进入auto_game_collection函数
    auto_gc = False

    # 如使用通用的游戏数据采集流程（auto_gc为True），则会根据该参数判定是否只获取OSA配置的伺服器的数据，默认False
    osa_server = False

    # 如使用通用的游戏数据采集流程，以下信息必须继承重写
    game_code = None  # 游戏代码，建议大写，类型为字符串
    platform = None  # 游戏所属平台，类型为字符串：晶绮（jq）、初心（cx）、和悦（hy）

    def _funny(self, response):
        """
        彩蛋流程专用，这个函数一般只用于彩蛋，不用于继承重写或参与业务
        """

        item = self.item(None, parse='_funny')
        yield item

    def start_requests(self):
        """
        1.构建初始请求对象并返回
        2.继承后，该方法可以重写，但必须使用关键字yield内置请求对象
        :return request:(type=Request) 初始请求对象
        """

        if not isinstance(self.start, list):
            raise CheckUnPass('建造器的start必须为list类型！')
        for info in self.start:
            if not isinstance(info, dict) or 'way' not in info.keys():
                raise CheckUnPass('建造器的start列表里元素必须为dict类型，且必须有“way”这对键值！')
            request = self.request(**info)
            yield request

    def parse(self, response):
        """
        1.解析响应并返回新的请求对象或者数据对象
        2.此方法大多数情况下会被业务建造器继承并重写，原生只是基本流通
        3.重写过后，必须使用关键字yield内置请求对象（送去调度器）或内置数据对象（送去管道）
        4.重写该方法必须要接收一个参数（无论业务使用与否），是响应对象
        :param response:(type=Response) 引擎回传的响应对象
        :return item:(type=Item,Request) 内置数据、请求对象
        """

        item = self.item(None)
        yield item

    def downloader_error_callback(self, e, request):
        """
        当下载器抛出异常后，会调用此函数，一般都需要业务建造器继承重写处理异常的逻辑
        :param e:(type=Exception) 下载器抛出的异常对象
        :param request:(type=Request) 交给下载器的请求对象
        """

        raise e

    def auto_game_collection(self, response=None):
        """
        1.使用通用的游戏数据采集流程
        2.通用流程：取公司平台SDK数据（在线除外） → 入库明细表
        3.此方法一般不需要继承重写，是固定的流程
        :param response:(type=Response) 引擎回传的响应对象，这个函数较为特殊，引擎初次调用时默认为None
        :return request:(type=Request) 内置请求对象
        """

        # 只获取OSA配置的伺服器的数据
        if self.osa_server and response is None:
            server_dict = cp.osa_server_dict(self.platform, self.game_code)
            server_dict['parse'] = 'auto_game_collection'
            yield self.request(**server_dict)

        # 注册、登录、储值
        if not self.osa_server or response is not None:
            server_data = response.data
            server, register_where, login_where = None, '', ''
            if self.osa_server and len(server_data):
                cf.print_log('（通用游戏数据采集流程）%s游戏只获取OSA配置的伺服器的数据！' % self.game_code)
                server, str_server = list(), list()
                for one_server in server_data:
                    server.append(str(one_server['servercode']))
                    str_server.append('"%s"' % one_server['servercode'])
                register_where = ' AND serid in (%s)' % ','.join(str_server)
                login_where = ' AND a.serid in (%s)' % ','.join(str_server)
            db_name = '%s_sdk' % self.platform  # 查询数据库
            str_format = '%Y-%m-%d %H:%M:%S'
            start, end = cp.time_quantum(dt_format=str_format)  # 开始与结束时间
            start = cf.change_time_format(start, before=str_format, after=str_format,
                                          interval=-3000)  # SDK延迟，开始时间推前50分钟
            info_list = [
                # 注册
                {'way': 'db', 'parse': 'auto_game_parse', 'meta': 'register', 'db_name': db_name,
                 'table': 'oper_game_user',
                 'columns': ['userid', 'comefrom', 'ipaddr', 'regdate', 'serid'],
                 'after_table': 'WHERE gamecode="%s" AND (regdate BETWEEN "%s" AND "%s")%s' % (
                     self.game_code, start, end, register_where)},
                # 登录
                {'way': 'db', 'parse': 'auto_game_parse', 'meta': 'login', 'db_name': db_name,
                 'table': 'oper_game_login AS a,oper_game_user AS b',
                 'columns': ['a.userid', 'a.comefrom', 'a.ipaddr', 'a.crtime', 'a.serid'],
                 'after_table': 'WHERE a.gamecode=b.gamecode AND a.serid=b.serid AND a.userid=b.userid AND '
                                'b.regdate<="%s" AND a.gamecode="%s" AND (a.indate BETWEEN "%s" AND "%s") AND ('
                                'a.crtime BETWEEN "%s" AND "%s")%s' % (
                                    end, self.game_code,
                                    cf.change_time_format(start, before=str_format, after='%Y-%m-%d'),
                                    cf.change_time_format(end, before=str_format, after='%Y-%m-%d'), start, end,
                                    login_where)}
            ]
            pay_info = cp.game_money_dict(self.platform, self.game_code, start, end, server=server)  # 储值
            pay_info['parse'] = 'auto_game_parse'
            info_list.append(pay_info)
            for info in info_list:
                request = self.request(**info)
                yield request

            # 在线，需自行编写
            # 在线要在后面获取，否则有概率发生引擎过快关闭的情况
            # meta带上的server在有数据的情况下为一个列表，里面元素为字符串，是OSA配置的伺服器列表
            request = self.request('test', parse='auto_online_collection', meta=server)
            yield request

    def auto_game_parse(self, response):
        """
        通用游戏数据采集流程，入库明细表
        :param response:(type=Response) 引擎回传的响应对象
        :return item:(type=Item) 入库明细表的内置数据对象
        """

        # 获取数据标识与源数据
        key = response.meta
        source_data = response.data
        cf.print_log('（通用游戏数据采集流程）获取到%s游戏的%s数据，数据长度%s！' % (self.game_code, key, len(source_data)))

        # 通用参数
        str_format = '%Y-%m-%d %H:%M:%S'

        # 在线
        if key == 'online':
            server_code = str(source_data['server_code'])
            time = source_data.get('time', launch['datetime'].strftime('%Y-%m-%d %H:%M:%S'))[:-4] + '0:00'
            count = int(source_data['count'])
            data = {
                'platform': self.platform,
                'source': {'gamecode': self.game_code, 'servercode': server_code, 'time': time,
                           'count': count}
            }
            yield self.item(data, detail=key)

        # 注册、登录
        elif key in ('register', 'login'):
            for one_data in source_data:
                user_id = str(one_data['userid'])
                ip = one_data['ipaddr']
                server_code = str(one_data['serid'])
                os = one_data['comefrom']
                time = one_data['regdate'].strftime(str_format) if key == 'register' else one_data['crtime'].strftime(
                    str_format)
                data = {
                    'platform': self.platform,
                    'source': {'gamecode': self.game_code, 'servercode': server_code, 'userid': user_id, 'ip': ip,
                               'os': os, 'time': time}
                }
                yield self.item(data, detail=key)
                if key == 'register':  # 注册还需要同时录入一条登录
                    yield self.item(data, detail='login')

        # 储值
        elif key == 'pay':
            for one_data in source_data:
                order_id = one_data['gd_orderid']
                server_code = str(one_data['servercode'])
                user_id = str(one_data['userid'])
                os = one_data['comefrom']
                time = one_data['create_time'].strftime(str_format)
                amt = one_data['epoint']
                ip = one_data['user_ip']
                data = {
                    'platform': self.platform,
                    'source': {'gamecode': self.game_code, 'order_id': order_id, 'servercode': server_code,
                               'userid': user_id, 'os': os, 'time': time, 'amt': amt, 'ip': ip}
                }
                yield self.item(data, detail=key)

    def auto_online_collection(self, response):
        """
        公司平台暂时没有在线数据，只能由原厂提供，需要继承重写此函数获取在线数据
        1.此函数一般都需要重写，除非原厂没提供获取在线数的方法，那就没有在线数据了
        2.可自定义新函数入库在线数据，也可以用默认函数
        3.如果使用默认函数入库，需遵守下面的规则：
            ① yield出去的Request对象，way属性为字符串test
            ② 续①，带上parse属性，为字符串auto_game_parse
            ③ 续①，带上meta属性，为字符串online
            ④ 续①，带上test_data属性，为一个字典，伺服器编码的key为server_code
            ⑤ 续④，时间节点的key为time，格式“%Y-%m-%d %H:%M:%S”
            ⑥ 续④，在线数的key为count，应该是一个数字
        :param response:(type=Response) 引擎回传的响应对象
        :return item:(type=Item,Request) 内置数据、请求对象
        """

        item = self.item(None)
        yield item

    @staticmethod
    def request(*args, **kwargs):
        """
        为业务建造器提供生成内置请求对象的接口
        :param args:(type=tuple) 生成请求对象的信息，可变参数
        :param kwargs:(type=dict) 生成请求对象的信息，关键字参数
        :return request:(type=Request) 内置请求对象
        """

        request = Request(*args, **kwargs)
        return request

    @staticmethod
    def item(*args, **kwargs):
        """
        为业务建造器提供生成内置数据对象的接口
        :param args:(type=tuple) 生成数据对象的信息，可变参数
        :param kwargs:(type=dict) 生成数据对象的信息，关键字参数
        :return item:(type=Item) 内置数据对象
        """

        item = Item(*args, **kwargs)
        return item
