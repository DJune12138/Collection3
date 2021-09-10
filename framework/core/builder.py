"""
建造器组件：
1.构建请求信息，生成请求对象
2.解析响应对象，返回数据对象或者新的请求对象
"""

import re
from datetime import datetime
from framework.object.request import Request
from framework.object.item import Item
from framework.error.check_error import CheckUnPass
from utils import common_profession as cp, common_function as cf
from services import launch, redis


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

    # 如使用通用的游戏数据采集流程，以下参数必须继承重写
    game_code = None  # 游戏代码，建议大写，类型为字符串
    platform = None  # 游戏所属平台，类型为字符串：晶绮（jq）、初心（cx）、和悦（hy）

    # 如使用通用的游戏数据采集流程，以下参数可选继承重写
    osa_server = False  # 根据该参数判定是否只获取OSA配置的伺服器的数据，默认False
    timezone = None  # 是否需要转换时区，如需要转换的时区，则填写str，格式“本地时区/目标时区”（例：+08:00/+09:00），默认None不转换时区

    # 是否自动生成游戏数据采集流程的旧报表
    # 目前旧报表需要调用旧的Python2脚本生成
    # 继承后可重写该属性，True为自动生成，False为不自动生成，默认False
    old_report = False

    def __new__(cls, *args, **kwargs):
        """
        判断是否需要自动生成游戏数据采集流程的旧报表，继承后不建议重写
        """

        if cls.old_report:
            setattr(cls, 'end_requests_1', cls.__def_old_report)
        return object.__new__(cls)

    def __def_old_report(self):
        """
        游戏数据采集流程生成旧报表，这个函数不用于继承重写
        """

        info = cp.old_analyse(self.platform, self.game_code)
        if info is None:
            return
        yield self.request(**info)

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
        1.注意，重写该方法时，如果要继续请求，需要return（这里不是yield）一个请求对象
        2.如果不return一个人请求对象，则会结束该条请求
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
            str_format = '%Y-%m-%d %H:%M:%S'
            start, end = cp.time_quantum(dt_format=str_format)  # 开始与结束时间
            cf.print_log('（通用游戏数据采集流程）%s 执行 %s ~ %s' % (self.game_code, start, end))
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
            start = cf.change_time_format(start, before=str_format, after=str_format,
                                          interval=-3000)  # SDK延迟，开始时间推前50分钟
            register_time, login_time, timezone_format, interval = 'regdate', 'a.crtime', '"%s"', 0
            if self.timezone is not None:  # 根据时区转换数据时间
                l_time = self.timezone.split('/')[0]  # 本地时区
                t_time = self.timezone.split('/')[1]  # 目标时区
                register_time = 'CONVERT_TZ(regdate,"%s","%s") AS regdate' % (l_time, t_time)
                login_time = 'CONVERT_TZ(a.crtime,"%s","%s") AS crtime' % (l_time, t_time)
                timezone_format = 'CONVERT_TZ("%s","{}","{}")'.format(t_time, l_time)
                re_timezone = re.match(r'([+,-])(\d+):(\d+)', l_time)
                symbol, hour, minute = re_timezone.group(1), re_timezone.group(2), re_timezone.group(3)
                l_second = int('%s%s' % (symbol, (int(hour) * 3600 + int(minute) * 60)))
                re_timezone = re.match(r'([+,-])(\d+):(\d+)', t_time)
                symbol, hour, minute = re_timezone.group(1), re_timezone.group(2), re_timezone.group(3)
                t_second = int('%s%s' % (symbol, (int(hour) * 3600 + int(minute) * 60)))
                interval = l_second - t_second
            info_list = [
                # 注册
                {'way': 'db', 'parse': 'auto_game_parse', 'meta': 'register', 'db_name': db_name,
                 'table': 'oper_game_user',
                 'columns': ['userid', 'comefrom', 'ipaddr', register_time, 'serid'],
                 'after_table': 'WHERE gamecode="%s" AND (regdate BETWEEN %s AND %s)%s' % (
                     self.game_code, timezone_format % start, timezone_format % end, register_where)},
                # 登录
                {'way': 'db', 'parse': 'auto_game_parse', 'meta': 'login', 'db_name': db_name,
                 'table': 'oper_game_login AS a,oper_game_user AS b',
                 'columns': ['a.userid', 'a.comefrom', 'a.ipaddr', login_time, 'a.serid'],
                 'after_table': 'WHERE a.gamecode=b.gamecode AND a.serid=b.serid AND a.userid=b.userid AND '
                                'b.regdate<=%s AND a.gamecode="%s" AND (a.indate BETWEEN "%s" AND "%s") AND ('
                                'a.crtime BETWEEN %s AND %s)%s' % (
                                    timezone_format % end, self.game_code,
                                    cf.change_time_format(start, before=str_format, after='%Y-%m-%d',
                                                          interval=interval),
                                    cf.change_time_format(end, before=str_format, after='%Y-%m-%d', interval=interval),
                                    timezone_format % start, timezone_format % end, login_where)}
            ]
            pay_info = cp.game_money_dict(self.platform, self.game_code, start, end, server=server,
                                          timezone=self.timezone)  # 储值
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
        if key != 'online':
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
                dup_column = 'regtime' if key == 'register' else 'logintime'
                data = {
                    'platform': self.platform,
                    'source': {'gamecode': self.game_code, 'servercode': server_code, 'userid': user_id, 'ip': ip,
                               'os': os, 'time': time, 'duplicates': [dup_column], 'dup_ac': True}
                }
                yield self.item(data, detail=key)
                if key == 'register':  # 注册还需要同时录入一条登录
                    data['source']['duplicates'] = ['logintime']
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

    def af_spend(self, response):
        """
        OSA上AF行销看板各类媒体实际花费数据处理入库
        1.发起该请求时，meta属性带上平台，data属性带上数据，而data的数据为一个dict并有如下key：
            ① game_code：游戏代码
            ② media：媒体名
            ③ osa_name：②的媒体名在osa的别称
            ④ device：设备，android、ios等
            ⑤ country：国家或地区代码，TW、KR、JP等
            ⑥ time：日期，兼容datetime类型
            ⑦ spend：花费数据
        2.由于af_spend中的字段item需要映射配置获取国家名称，因此此方法有两个去向：
            ① 先获取redis有没有缓存，如有缓存则可以直接分析入库
            ② 如没有缓存，则先分析，再发起“country_code_name”请求，再入库
        :param response:(type=Response) 引擎回传的响应对象
        :return item:(type=Item,Request) 内置数据、请求对象
        """

        # 获取传输数据
        platform = response.meta
        source_data = response.data

        # 分析数据
        item_data = {'db_name': 'osa_%s' % platform, 'table': 'af_spend', 'values': None,
                     'columns': ['gamecode', 'media', 'osa_name', 'device', 'country', 'time', 'item', 'spend'],
                     'duplicates': ['spend']}
        device = source_data['device'].lower()
        if device == 'android':
            device_name = '安卓'
        elif device == 'ios':
            device_name = 'IOS'
        else:
            device_name = 'other'
        country = source_data['country'].upper()
        country = 'OTHER' if not country or country in ('NONE', 'UNKNOWN') else country
        country_name = '其他' if country == 'OTHER' else redis['127_0'].get('%s_country_name' % country)
        time = source_data['time']
        if isinstance(time, datetime):
            time = time.strftime('%Y-%m-%d')
        values = [source_data['game_code'], source_data['media'], source_data['osa_name'], device,
                  country, time, '%s-' % device_name, source_data['spend']]
        item_data['values'] = values

        # 有地区名称缓存则直接入库
        if country_name is not None:
            values[6] += country_name
            yield self.item(item_data)

        # 没有则发起查询地区的请求
        else:
            yield self.request('db', parse='af_spend_country', meta=item_data, db_name='osa_jq', table='area_code_list',
                               after_table='WHERE countrycode="%s"' % country, fetchall=False)

    def af_spend_country(self, response):
        """
        获取af_spend里item字段所需的国家地区明，再入库数据
        :param response:(type=Response) 引擎回传的响应对象
        :return item:(type=Item,Request) 内置数据、请求对象
        """

        # 提取数据
        into_db_data = response.meta
        country_data = response.data
        if country_data:
            country = country_data['countrycode'].upper()
            country_name = country_data['countryname']
        else:
            country = into_db_data['values'][4]
            country_name = country

        # 添加缓存
        # 一般来说地区名字不会变且齐全，但如果太皮。。还是加个7天的过期时间吧
        item_data = {'db_type': 'redis', 'db_name': '127_0', 'key': '%s_country_name' % country, 'value': country_name,
                     'ex': 604800}
        yield self.item(item_data)

        # 修正item，入库
        into_db_data['values'][6] += country_name
        yield self.item(into_db_data)

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
