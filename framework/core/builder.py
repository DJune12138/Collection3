"""
建造器组件：
1.构建请求信息，生成请求对象
2.解析响应对象，返回数据对象或者新的请求对象
"""

from re import match
from threading import Lock
from datetime import datetime, timedelta
import services
import config
from utils import common_profession as cp, common_function as cf
from framework.object.request import Request
from framework.object.item import Item
from framework.error.check_error import CheckUnPass


class Builder(object):
    """
    建造器组件
    """

    # 建造器名称
    # 继承类后重写该属性即可，必须重写
    # 用于识别业务，每个业务应为唯一名称，必须使用字符串
    name = None

    # 默认初始请求
    # 继承类后重写该属性即可，必须为list类型（或str类型，启用特殊功能）
    # 目前只有start_requests方法会调用该属性，如继承类后重写了start_requests方法，可以不需要该属性
    # 元素必须为dict类型，且必须有“way”这对键值
    # 原生start_requests函数会把列表中每一个mapping作为关键字参数构建请求对象
    # str类型为启用特殊功能，具体功能详见start_requests函数
    start = [{'way': 'test', 'parse': '_funny'}]

    # 是否使用通用的游戏数据采集流程
    # 继承类后可重写该属性，True为使用，False为不使用，不重写则默认不使用
    # 如果使用通用流程，则不再进入start_requests函数，而进入auto_game_collection函数
    auto_gc = False

    # 如使用通用的游戏数据采集流程，以下参数必须继承重写
    game_code = None  # 游戏代码，建议大写，类型为字符串
    platform = None  # 游戏所属平台，类型为字符串：晶绮（jq）、初心（cx）、和悦（hy）

    # 如使用通用的游戏数据采集流程，以下参数可选继承重写
    osa_server = False  # 根据该参数判定是否只获取OSA配置的伺服器的数据
    timezone = None  # 是否需要转换时区，如需要转换的时区，则填写str，格式“本地时区/目标时区”，例：+08:00/+09:00
    auto_pass = None  # 跳过自动采集register、login或pay以供后续个性化定制，例：["register"]、["register", "login", "pay"]

    # 是否自动生成游戏数据采集流程的旧版报表
    # 由于OSA设计问题，旧版报表需要另外生成，该参数设置为True可自动生成旧版报表
    # 传入True则在end_requests_1函数执行，传入int类型则在对应编号的end_requests函数执行
    old_report = False

    # 入库明细时反查IP或OS
    # 有部分数据来源可能无法提供IP或OS信息，只能通过反查注册平台账号时的信息来写入
    # 注册register、登录login、储值pay可分别反查，根据需要填入字段
    # 例：{"register": ["ip"], "login": ["ip", "os"], "pay": ["os"]}
    # 如果要启用替换华为os功能，则为os_hw，例：{"register": ["os_hw"]}
    pegging_field = None

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

    @property
    def cf(self):
        """
        为业务建造器提供框架cf模块的接口
        :return item:(type=module) cf模块对象
        """

        return cf

    @property
    def cp(self):
        """
        为业务建造器提供框架cp模块的接口
        :return item:(type=module) cp模块对象
        """

        return cp

    @property
    def services(self):
        """
        为业务建造器提供框架services模块的接口
        :return item:(type=module) services模块对象
        """

        return services

    @property
    def config(self):
        """
        为业务建造器提供框架config模块的接口
        :return item:(type=module) config模块对象
        """

        return config

    def start_requests(self):
        """
        1.构建初始请求对象并返回
        2.继承后，该方法可以重写，但必须使用关键字yield内置请求对象
        """

        # 根据start类型开启功能
        # list类型为正常类型，直接执行
        if isinstance(self.start, list):
            start = self.start

        # str类型会启用特殊功能
        elif isinstance(self.start, str):
            if self.start == 'osa_server':  # 查询OSA配置伺服器
                start = [cp.osa_server_dict(self.platform, self.game_code)]
            else:
                raise CheckUnPass('建造器的start目前支持的功能有：1.osa_server（查询OSA配置伺服器）')

        # 其余类型均不支持，报错
        else:
            raise CheckUnPass('建造器的start正常使用必须为list类型！如启用特殊功能可传入str类型，具体功能请查看建造器start_requests函数。')

        # 执行初始请求
        for info in start:
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
        """

        item = self.item(None)
        yield item

    def downloader_error_callback(self, e, request):
        """
        当下载器抛出异常后，会调用此函数，一般都需要业务建造器继承重写处理异常的逻辑
        1.注意，重写该方法时，如果要继续请求，需要return（这里不是yield）一个请求对象
        2.如果不是return一个请求对象，则会结束该条请求
        :param e:(type=Exception) 下载器抛出的异常对象
        :param request:(type=Request) 交给下载器的请求对象
        """

        raise e

    def _funny(self, response):
        """
        彩蛋流程专用，这个函数一般只用于彩蛋，不用于继承重写或参与业务
        """

        item = self.item(None, parse='_funny')
        yield item

    def __pegging_data(self, key, user_id, ip, os):
        """
        1.自用函数，这个函数不用于继承重写
        2.函数功能为反查IP或OS
        :param key:(type=str) 数据类型，register、login、pay
        :param user_id:(type=str) 要反查的账户id
        :param ip:(type=str,None) 原始获取的IP
        :param os:(type=str,None) 原始获取的OS
        :return ip:(type=str,None) 符合条件后反查的IP，不符合则原始IP
        :return os:(type=str,None) 符合条件后反查的OS，不符合则原始OS
        """

        # 启用转换
        if self.pegging_field is not None:
            pegging_value = self.pegging_field.get(key)

            # 配置了key的才转
            if pegging_value is not None:
                pegging_result = cp.u_pegging(self.platform, [user_id], pegging_value)
                ip = pegging_result.get(user_id, dict()).get('ip', ip)
                os = pegging_result.get(user_id, dict()).get('os', os)

        # 返回结果
        return ip, os

    def auto_game_collection(self, response=None):
        """
        1.使用通用的游戏数据采集流程
        2.通用流程：取公司平台SDK数据（在线除外） → 入库明细表
        3.此方法一般不需要继承重写，是固定的流程
        :param response:(type=Response) 引擎回传的响应对象，这个函数较为特殊，引擎初次调用时默认为None
        """

        # 只获取OSA配置的伺服器的数据
        if self.osa_server and response is None:
            server_dict = cp.osa_server_dict(self.platform, self.game_code)
            server_dict['parse'] = 'auto_game_collection'
            yield self.request(**server_dict)

        # 注册、登录、储值
        if not self.osa_server or response is not None:
            start, end, start_offset = cp.time_quantum(dt_format=config.format_datetime_n, start_offset=True)
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
            register_time, login_time, timezone_format, interval = 'regdate', 'a.crtime', '"%s"', 0
            if self.timezone is not None:  # 根据时区转换数据时间
                l_time = self.timezone.split('/')[0]  # 本地时区
                t_time = self.timezone.split('/')[1]  # 目标时区
                register_time = 'CONVERT_TZ(regdate,"%s","%s") AS regdate' % (l_time, t_time)
                login_time = 'CONVERT_TZ(a.crtime,"%s","%s") AS crtime' % (l_time, t_time)
                timezone_format = 'CONVERT_TZ("%s","{}","{}")'.format(t_time, l_time)
                re_timezone = match(r'([+,-])(\d+):(\d+)', l_time)
                symbol, hour, minute = re_timezone.group(1), re_timezone.group(2), re_timezone.group(3)
                l_second = int('%s%s' % (symbol, (int(hour) * 3600 + int(minute) * 60)))
                re_timezone = match(r'([+,-])(\d+):(\d+)', t_time)
                symbol, hour, minute = re_timezone.group(1), re_timezone.group(2), re_timezone.group(3)
                t_second = int('%s%s' % (symbol, (int(hour) * 3600 + int(minute) * 60)))
                interval = l_second - t_second
            info_list = [
                # 注册
                {'way': 'db', 'parse': 'auto_game_parse', 'meta': 'register', 'db_name': db_name,
                 'table': 'oper_game_user',
                 'columns': ['userid', 'comefrom', 'ipaddr', register_time, 'serid'],
                 'after_table': 'WHERE gamecode="%s" AND (regdate BETWEEN %s AND %s)%s' % (
                     self.game_code, timezone_format % start_offset, timezone_format % end, register_where)},
                # 登录
                {'way': 'db', 'parse': 'auto_game_parse', 'meta': 'login', 'db_name': db_name,
                 'table': 'oper_game_login AS a,oper_game_user AS b',
                 'columns': ['a.userid', 'a.comefrom', 'a.ipaddr', login_time, 'a.serid'],
                 'after_table': 'WHERE a.gamecode=b.gamecode AND a.serid=b.serid AND a.userid=b.userid AND '
                                'b.regdate<=%s AND a.gamecode="%s" AND (a.indate BETWEEN "%s" AND "%s") AND ('
                                'a.crtime BETWEEN %s AND %s)%s' % (
                                    timezone_format % end, self.game_code,
                                    cf.change_time_format(start_offset, before=config.format_datetime_n,
                                                          after=config.format_date_n, interval=interval),
                                    cf.change_time_format(end, before=config.format_datetime_n,
                                                          after=config.format_date_n, interval=interval),
                                    timezone_format % start_offset, timezone_format % end, login_where)}
            ]
            pay_info = cp.game_money_dict(self.platform, self.game_code, start_offset, end, server=server,
                                          timezone=self.timezone)  # 储值
            pay_info['parse'] = 'auto_game_parse'
            info_list.append(pay_info)
            for info in info_list:
                key = info['meta']
                if self.auto_pass is None or key not in self.auto_pass:  # 跳过部分自动采集
                    request = self.request(**info)
                    yield request
                else:
                    cf.print_log('（通用游戏数据采集流程）跳过%s的采集！' % key)

            # 个性化定制部分数据（online、register、login、pay）的采集，需自行编写
            # 要在后面获取，否则有概率发生引擎过快关闭的情况
            # meta带上的server在有数据的情况下为一个列表，里面元素为字符串，是OSA配置的伺服器列表
            # meta还会带上开始时间start和结束时间end
            meta = {'start': start_offset, 'end': end, 'server': server}
            request = self.request('test', parse='auto_collection_personalized', meta=meta)
            yield request

    def auto_game_parse(self, response):
        """
        通用游戏数据采集流程，入库明细表
        :param response:(type=Response) 引擎回传的响应对象
        """

        # 获取数据标识与源数据
        # 可通过传参自定义game_code
        # 1.要使用自定义game_code，则在meta传参使用字典，并带上“key”与“game_code”键值对
        # 2.如不需要自定义game_code，则meta直接使用字符串代表“key”
        meta = response.meta
        if isinstance(meta, dict):
            key = meta['key']
            game_code = meta['game_code']
        else:
            key = meta
            game_code = self.game_code
        source_data = response.data
        cf.print_log('（通用游戏数据采集流程）获取到%s游戏的%s数据，数据长度%s！' % (game_code, key, len(source_data)))

        # 在线
        if key == 'online':
            for one_data in source_data:
                server_code = str(one_data['server_code'])
                time = one_data.get('time', services.launch['datetime'].strftime(
                    config.format_datetime_n))[:-4] + '0:00'
                count = int(one_data['count'])
                data = {
                    'platform': self.platform,
                    'source': {'gamecode': game_code, 'servercode': server_code, 'time': time, 'count': count,
                               'duplicates': ['online_count']}
                }
                yield self.item(data, detail=key)

        # 注册、登录
        elif key in ('register', 'login'):
            for one_data in source_data:
                user_id = str(one_data['userid'])
                server_code = str(one_data['serid'])
                ip = one_data.get('ipaddr')
                os = one_data.get('comefrom')
                ip, os = self.__pegging_data(key, user_id, ip, os)  # 反查
                time = one_data['regdate'].strftime(config.format_datetime_n) if key == 'register' else one_data[
                    'crtime'].strftime(config.format_datetime_n)
                dup_column = 'regtime' if key == 'register' else 'logintime'
                data = {
                    'platform': self.platform,
                    'source': {'gamecode': game_code, 'servercode': server_code, 'userid': user_id, 'ip': ip,
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
                ip = one_data.get('user_ip')
                os = one_data.get('comefrom')
                ip, os = self.__pegging_data(key, user_id, ip, os)  # 反查
                time = one_data['create_time'].strftime(config.format_datetime_n)
                amt = one_data['epoint']
                data = {
                    'platform': self.platform,
                    'source': {'gamecode': game_code, 'order_id': order_id, 'servercode': server_code,
                               'userid': user_id, 'os': os, 'time': time, 'amt': amt, 'ip': ip}
                }
                yield self.item(data, detail=key)

    def auto_collection_personalized(self, response):
        """
        个性化定制部分数据（online、register、login、pay）的采集
        1.公司平台暂时没有online数据，如原厂提供了该数据，需要继承重写此函数获取
        2.除了online数据，register、login、pay数据在需要个性化定制采集时也可以继承重写
        3.可自定义新函数入库数据，也可以用默认函数（auto_game_parse）
        4.如果使用默认函数入库，需遵守下面的规则：
            ① yield出去的Request对象，way属性为字符串test
            ② 续①，带上parse属性，为字符串auto_game_parse
            ③ 续①，带上meta属性，为字符串online、register、login、pay其一
        :param response:(type=Response) 引擎回传的响应对象
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
        country_name = '其他' if country == 'OTHER' else services.redis['127_0'].get('%s_country_name' % country)
        time = source_data['time']
        if isinstance(time, datetime):
            time = time.strftime(config.format_date_n)
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

    def __new__(cls, *args, **kwargs):
        """
        1.判断是否启用自动生成游戏数据采集流程旧版报表的功能
        2.当判断为启用该功能后，如果“just-do”传参为“orp”（即“-jorp”），会跳过所有业务逻辑，直接生成旧版报表
        3.如果涉及到这些业务逻辑，不建议业务建造器重写__new__函数
        :param args:(type=tuple) 可变参数
        :param kwargs:(type=dict) 关键字参数
        :return obj:(type=object) 加工过后的新式类
        """

        # 判断启用功能
        if cls.old_report and isinstance(cls.old_report, int):  # 温馨提示：Python3里True==1，True会视为int类型
            if cls.old_report == 1:  # True虽然==1，但是转为字符串时不为1
                setattr(cls, 'end_requests_1', cls.__def_old_report)
            else:
                setattr(cls, 'end_requests_%s' % cls.old_report, cls.__def_old_report)

            # 传参“-jorp”则覆盖所有业务逻辑
            if cp.get_argv(config.pk_jd) == 'orp':
                cf.print_log('跳过所有业务逻辑，直接生成旧版报表！')
                setattr(cls, 'start_requests', cls.__empty_requests)
                if cls.auto_gc:
                    setattr(cls, 'auto_game_collection', cls.__empty_requests)
                for i in range(1, cls.old_report):
                    setattr(cls, 'end_requests_%s' % i, cls.__empty_requests)

        # 返回类
        obj = object.__new__(cls)
        return obj

    def __empty_requests(self):
        """
        1.当游戏数据采集流程只生成旧版报表时，覆盖业务建造器的start_requests与所有end_requests
        2.覆盖后的函数只会返回一个彩蛋请求，然后可直接生成旧版报表
        3.会根据old_report参数配置来覆盖，详见old_report参数的注释说明
        4.该函数不用于继承重写
        """

        yield self.request(way='test', parse='_funny')

    def __def_old_report(self):
        """
        游戏数据采集流程生成旧版报表，这个函数不用于继承重写
        1.根据传参“pass-something”判断是否生成旧版报表，“orp”（即“-porp”）则不生成旧版报表
        2.有的游戏可能会需要自定义是否生成旧版报表的条件，如有该需求则仿照此处写法yield请求即可
        """

        if cp.get_argv(config.pk_ps) != 'orp':
            yield self.request(way='test', parse='osa_report')

    def osa_report(self, response):
        """
        查询旧版报表所需的在线、注册、登录、储值四种数据
        1.时间传参开始与结束的“%H%M”都为“0000”的话只执行日报表，其余情况两种报表均执行
        2.正常定时任务（不带任何时间传参）跳过第一个节点的实时报表，减少算力浪费
        :param response:(type=Response) 引擎回传的响应对象
        """

        # 获取传参参数与业务参数
        # 可自定义game_code与自定义生成报表的节点，只需要meta参数带上即可
        # 1.meta参数为dict类型，带上“game_code”、“start”、“end”键值对
        # 2.“game_code”为str类型，“start”与“end”的值均为datetime类型
        start, end = cp.time_quantum()
        meta = response.meta
        if isinstance(meta, dict):
            game_code = meta.get('game_code', self.game_code)
            start_dt, end_dt = meta.get('start', start), meta.get('end', end)
        else:
            game_code, start_dt, end_dt = self.game_code, start, end
        argv_st = cp.get_argv(config.pk_st, transform=True)  # 仅作用于判断是否生成实时报表，和是否需要跳过实时报表首节点，下同
        argv_et = cp.get_argv(config.pk_et, transform=True)
        db_name = 'osa_%s' % self.platform
        cf.print_log('准备生成旧版报表...')

        # 查询SQL语句模板
        online_sql_realtime = """SELECT a.gamecode,b.gamename,a.servercode,a.online_count num
        FROM oper_game_online a LEFT JOIN game_group b ON a.gamecode=b.gamecode
        WHERE a.gamecode="%s" AND a.online_time="{}";""" % game_code  # 实时报表在线，查明细
        online_sql_day = """SELECT a.gamecode,b.gamename,a.servercode,MAX(a.online_count) AS num
        FROM oper_game_online a LEFT JOIN game_group b ON a.gamecode=b.gamecode
        WHERE a.gamecode="%s" AND a.online_time BETWEEN "{} 00:00:00" AND "{} 23:50:00"
        GROUP BY a.servercode;""" % game_code  # 日报表在线，查单服最大在线（PCU）
        rlp_sql = """SELECT a.gamecode,b.gamename,a.servercode,COUNT(DISTINCT a.userid) AS num{}
        FROM oper_game_{} a LEFT JOIN game_group b ON a.gamecode=b.gamecode
        WHERE a.gamecode="%s" AND a.{}date="{}"{} GROUP BY 3;""" % game_code  # 注册、登录、储值，{}date字段作用是索引
        realtime_where = ' AND {}time BETWEEN "{} 00:00:00" AND "{}"'  # 实时报表的条件补充

        # 实时报表
        if not argv_st.endswith('0000') or not argv_et.endswith('0000'):
            is_pass = True if not argv_st and not argv_et else False  # 辅助标记跳过首个节点
            rdt = start_dt
            while rdt <= end_dt:
                rdt_str = rdt.strftime(config.format_datetime_n)
                rd_str = rdt.strftime(config.format_date_n)
                rdt += timedelta(minutes=config.gc_interval)
                if is_pass:
                    is_pass = False
                    continue

                # 由于旧报表算法需要四种数据都查出来后再加工计算，需要锁机制辅助判断数据是否齐全
                lock = Lock()
                auxiliary_dict = dict()  # 辅助记录该节点四种数据完整性

                # 查询在线数据
                sql = online_sql_realtime.format(rdt_str)
                meta = {'lock': lock, 'dict': auxiliary_dict, 'key': 'online', 'datetime': rdt_str}
                request = {'way': 'db', 'parse': 'make_old', 'meta': meta, 'db_name': db_name, 'sql': sql}
                yield self.request(**request)

                # 注册
                sql = rlp_sql.format('', 'user', 'reg', rd_str, realtime_where.format('reg', rd_str, rdt_str))
                meta = {'lock': lock, 'dict': auxiliary_dict, 'key': 'register', 'datetime': rdt_str}
                request = {'way': 'db', 'parse': 'make_old', 'meta': meta, 'db_name': db_name, 'sql': sql}
                yield self.request(**request)

                # 登录
                sql = rlp_sql.format('', 'login', 'login', rd_str, realtime_where.format('login', rd_str, rdt_str))
                meta = {'lock': lock, 'dict': auxiliary_dict, 'key': 'login', 'datetime': rdt_str}
                request = {'way': 'db', 'parse': 'make_old', 'meta': meta, 'db_name': db_name, 'sql': sql}
                yield self.request(**request)

                # 储值
                sql = rlp_sql.format(',SUM(a.amt) AS amt', 'pay', 'pay', rd_str,
                                     realtime_where.format('pay', rd_str, rdt_str))
                meta = {'lock': lock, 'dict': auxiliary_dict, 'key': 'pay', 'datetime': rdt_str}
                request = {'way': 'db', 'parse': 'make_old', 'meta': meta, 'db_name': db_name, 'sql': sql}
                yield self.request(**request)

        # 日报表
        rd, ed = start_dt.date(), end_dt.date()
        while rd <= ed:
            rd_str = rd.strftime(config.format_date_n)
            rd += timedelta(days=1)
            lock, auxiliary_dict = Lock(), dict()
            sql = online_sql_day.format(rd_str, rd_str)  # 在线
            meta = {'lock': lock, 'dict': auxiliary_dict, 'key': 'online', 'date': rd_str}
            yield self.request(**{'way': 'db', 'parse': 'make_old', 'meta': meta, 'db_name': db_name, 'sql': sql})
            sql = rlp_sql.format('', 'user', 'reg', rd_str, '')  # 注册
            meta = {'lock': lock, 'dict': auxiliary_dict, 'key': 'register', 'date': rd_str}
            yield self.request(**{'way': 'db', 'parse': 'make_old', 'meta': meta, 'db_name': db_name, 'sql': sql})
            sql = rlp_sql.format('', 'login', 'login', rd_str, '')  # 登录
            meta = {'lock': lock, 'dict': auxiliary_dict, 'key': 'login', 'date': rd_str}
            yield self.request(**{'way': 'db', 'parse': 'make_old', 'meta': meta, 'db_name': db_name, 'sql': sql})
            sql = rlp_sql.format(',SUM(a.amt) AS amt', 'pay', 'pay', rd_str, '')  # 储值
            meta = {'lock': lock, 'dict': auxiliary_dict, 'key': 'pay', 'date': rd_str}
            yield self.request(**{'way': 'db', 'parse': 'make_old', 'meta': meta, 'db_name': db_name, 'sql': sql})

    def make_old(self, response):
        """
        获取旧版报表所需的四种数据，计算各种数值，生成旧版报表数据
        :param response:(type=Response) 引擎回传的响应对象
        """

        # 获取数据，利用锁机制辅助判断数据完整性
        meta, data = response.meta, response.data
        lock, tool_dict, key = meta['lock'], meta['dict'], meta['key']
        date_or_time = meta.get('datetime')
        if date_or_time is None:
            date_or_time, report_type = meta['date'], 'day'
        else:
            report_type = 'realtime'
        with lock:
            whole = False  # 完整性标记
            tool_dict[key] = data
            online, register, login, pay = tool_dict.get('online'), tool_dict.get('register'), tool_dict.get(
                'login'), tool_dict.get('pay')
            if online is not None and register is not None and login is not None and pay is not None:
                whole = True

        # 数据完整，开始分析计算
        # 由于需要每个服务器数据分别独立计算，要各种数据先遍历归类服务器
        if whole:
            all_data = {'server_data': dict()}
            for key, data in tool_dict.items():
                for one in data:
                    game_code, game_name, server_code = one['gamecode'], one['gamename'], str(one['servercode'])
                    all_data.setdefault('game_code', game_code)
                    all_data.setdefault('game_name', game_name)
                    server_data = all_data['server_data'].setdefault(server_code, dict())
                    server_data[key] = one['num']
                    if key == 'pay':
                        server_data['amt'] = round(float(one['amt']), 2)

            # 服务器数据归类完整，构造入库数据
            all_server_data = all_data['server_data']
            if all_server_data:
                game_code, game_name = all_data['game_code'], all_data['game_name']
                total_server = {'online': 0, 'register': 0, 'login': 0, 'pay': 0, 'amt': 0.0}  # 日报表的全服汇总数据
                db_name = 'osa_%s' % self.platform
                item_data = {'db_name': db_name, 'insert_limit': self.name, 'values': list(), 'limit_line': 50,
                             'columns': ['game_name', 'gamecode', 'servercode', 'serid', 'date', 'num', 'pay_per',
                                         'add_user_num', 'pay_num', 'pay_amount', 'arpu']}
                table = ''
                if report_type == 'realtime':
                    table = 'oper_analyze_day'
                    item_data['columns'].append('onlineplayer')
                elif report_type == 'day':
                    table = 'oper_analyze_perday'
                    item_data['columns'].append('pcu')
                item_data['table'] = table
                for server_code, server_data in all_server_data.items():
                    online_count = server_data.get('online', 0)  # 在线数
                    register_count = server_data.get('register', 0)  # 注册数
                    login_count = server_data.get('login', 0)  # 登录数
                    pay_count = server_data.get('pay', 0)  # 储值数
                    amt = server_data.get('amt', 0.0)  # 储值金额
                    pay_per = round(pay_count / login_count * 100, 2) if login_count else 0.0  # 付费比
                    arpu = round(amt / pay_count, 2) if pay_count else 0.0  # ARPU
                    one = [game_name, game_code, server_code, server_code, date_or_time, login_count, pay_per,
                           register_count, pay_count, amt, arpu, online_count]
                    item_data['values'].append(one)

                    # 日报表汇总全服
                    if report_type == 'day':
                        total_server['online'] += online_count
                        total_server['register'] += register_count
                        total_server['login'] += login_count
                        total_server['pay'] += pay_count
                        total_server['amt'] += amt
                if report_type == 'day':
                    pay_count, login_count, amt = total_server['pay'], total_server['login'], total_server['amt']
                    pay_per = round(pay_count / login_count * 100, 2) if login_count else 0.0
                    arpu = round(amt / pay_count, 2) if pay_count else 0.0
                    one = [game_name, game_code, 'all', '全服', date_or_time, login_count, pay_per,
                           total_server['register'], pay_count, amt, arpu, total_server['online']]
                    item_data['values'].append(one)

                # 更新数据，即先删除旧数据再入库
                sql = 'DELETE FROM %s WHERE gamecode="%s" AND date="%s";' % (table, game_code, date_or_time)
                meta = {'item_data': item_data, 'report_type': report_type, 'date_or_time': date_or_time}
                yield self.request('db', parse='update_old', meta=meta, db_name=db_name, db_limit=self.name, sql=sql)

    def update_old(self, response):
        """
        更新旧版报表数据，已删除旧数据，入库新数据
        :param response:(type=Response) 引擎回传的响应对象
        """

        meta = response.meta
        item_data, report_type, date_or_time = meta['item_data'], meta['report_type'], meta['date_or_time']
        cf.print_log('生成%s报表：%s' % (report_type, date_or_time))
        yield self.item(item_data)
