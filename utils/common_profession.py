"""
业务公共函数
"""

import os
import json
from datetime import datetime
from copy import deepcopy
from geoip2 import database as geoip2_db, errors as geoip2_e
import services  # 该模块在加载服务前就已经被导入，只能导入总模块，否则所有服务都会是加载前的None
from config import ding_token, factory_config, factory_code, geoip2_path, pk_st, pk_et, gc_interval, format_date, \
    format_datetime, account_name
from utils import common_function as cf
from framework.error.check_error import CheckUnPass


def send_ding(msg, group, e=None):
    """
    发送钉钉消息
    :param msg:(type=str) 要发送的钉钉信息内容
    :param group:(type=str) 要发送的钉钉群组
    :param e:(type=Exception) 报错对象，默认None则不使用报错模板
    :return result:(type=bool) 发送结果，成功为True，失败为False
    """

    # 校验group
    group_list = ding_token.keys()
    if group not in group_list:
        raise ValueError('group只能为%s其中一个！' % '、'.join(group_list))

    # 钉钉消息内容
    if e is not None:
        log_path = factory_config[factory_code]['logger_config.log_path']
        log_path = log_path if log_path is not None else os.path.join(os.getcwd(), 'log')
        ding_msg = '任务出错了！详情请查看报错日志！\n执行任务的主机IP：%s\n报错日志路径：%s\nMsg：%s\nError：%s' \
                   % (services.launch['ip'], log_path, msg, str(e))
    else:
        ding_msg = msg

    # 发送钉钉消息
    url = 'https://oapi.dingtalk.com/robot/send?access_token=%s' % ding_token[group]
    data = {
        'msgtype': 'text',
        'text': {
            'content': ding_msg
        }
    }
    headers = {
        'Content-Type': 'application/json;charset=utf-8'
    }
    response = cf.repetition_json(url, method='post', headers=headers, data=json.dumps(data))
    if response['errcode'] == 0:
        cf.print_log('钉钉消息发送成功！')
        result = True
    else:
        cf.print_log('钉钉消息发送失败！')
        result = False

    # 返回发送结果
    return result


def game_money_dict(platform, game_code, start, end, server=None, timezone=None):
    """
    构造获取平台储值的字典，用于构造请求
    :param platform:(type=str) 哪个平台，晶绮jq、和悦hy、初心cx
    :param game_code:(type=str) 游戏代码
    :param start:(type=str) 查询时间段的开始时间
    :param end:(type=str) 查询时间段的结束时间
    :param server:(type=list,tuple) 指定伺服器，注意里面的元素必须要str类型，默认None不指定
    :param timezone:(type=str) 是否需要转换时区，如需要转换的时区，格式“本地时区/目标时区”（例：+08:00/+09:00），默认None不转换时区
    :return request_dict:(type=dict) 用于构造内置请求对象所用参数的字典
    """

    # 时区转换
    time_column, timezone_format = 'a.create_time', '"%s"'
    if timezone is not None:
        l_time = timezone.split('/')[0]  # 本地时区
        t_time = timezone.split('/')[1]  # 目标时区
        time_column = 'CONVERT_TZ(a.create_time,"%s","%s") create_time' % (l_time, t_time)
        timezone_format = 'CONVERT_TZ("%s","{}","{}")'.format(t_time, l_time)

    # 固定参数
    way = 'db'
    meta = 'pay'  # 储值标识
    db_name = '%s_realtime' % platform
    table = 'stored_value_record a LEFT JOIN game_user b ON a.userid=b.userid'
    columns = ['a.gd_orderid', 'a.servercode', 'a.epoint', 'a.userid', time_column, 'b.comefrom', 'b.ipaddr user_ip']

    # 伺服器
    if server:
        server_list = ['"%s"' % one_server for one_server in server]
        where_server = ' AND a.servercode IN (%s)' % ','.join(server_list)
    else:
        where_server = ''

    # 查询条件
    after_table = 'WHERE a.gamecode="%s" AND (a.create_time BETWEEN %s AND %s) AND a.status IN (1,4)%s' % (
        game_code, timezone_format % start, timezone_format % end, where_server)

    # 构造字典并返回
    request_dict = {'way': way, 'meta': meta, 'db_name': db_name, 'table': table, 'columns': columns,
                    'after_table': after_table}
    return request_dict


def u_pegging(platform, uid_list, type_list):
    """
    根据userid反查数据，比如IP地址、系统
    :param platform:(type=str) 哪个平台，晶绮jq、和悦hy、初心cx
    :param uid_list:(type=list,set) 要反查的user_id列表
    :param type_list:(type=list) 反查类型的列表，os为系统，ip为IP地址，os_hw为启用替换华为os功能
    :return p_data:(type=dict) 反查的结果，key为user_id，value为数据dict，例：{"12345": {"os": "IOS", "ip": "127.0.0.1"}}
    """

    # 初始化参数
    column_map = {'os': 'comefrom', 'ip': 'ipaddr'}  # MySQL字段映射
    redis_ex = 604800  # 缓存在Redis的时间，7d*24h*60m*60s=604800s
    mysql_max = 100  # MySQL每批反查数量
    p_data = dict()  # 最终数据

    # 数据库连接
    mysql = services.mysql['%s_realtime' % platform]
    redis = services.redis['127_8']

    # 替换华为os功能
    os_hw = False
    if 'os_hw' in type_list:
        type_list = deepcopy(type_list)
        os_hw = True
        type_list.remove('os_hw')
        if 'os' not in type_list:
            type_list.append('os')

    # 1.从Redis根据user_id查
    none = set()  # Redis没查出来的user_id集合
    for user_id in uid_list:
        user_id = str(user_id)
        for type_ in type_list:
            data = redis.get('%s-%s' % (user_id, type_))  # 从Redis查
            if data is None:  # 查不出来就记录
                none.add(user_id)
            else:  # 查得出来就覆盖数据集合
                p_data.setdefault(user_id, dict())[type_] = data

    # 2.Redis查不出的再从MySQL分批查
    none = list(none)
    len_ = len(none)
    for i in range(int(len_ / mysql_max) if not len_ % mysql_max else int(len_ / mysql_max) + 1):
        columns = ['userid'] + [column_map[type_] for type_ in type_list]
        if os_hw:
            columns += ['platform']
        sql_data = mysql.select('game_user', columns=columns, after_table='WHERE userid IN (%s)' % (
            ','.join(none[i * mysql_max: i * mysql_max + mysql_max])))
        for one_data in sql_data:
            user_id = str(one_data['userid'])
            for type_ in type_list:
                if os_hw and type_ == 'os':
                    if 'ios' in one_data['comefrom'].lower():
                        data = 'IOS'
                    else:
                        data = 'hw' if one_data['platform'] == 'Huawei' else 'Android'
                else:
                    data = one_data[column_map[type_]]
                    if type_ == 'os':
                        data = 'IOS' if 'ios' in data.lower() else 'Android'  # os暂时只分为安卓与苹果
                p_data.setdefault(user_id, dict())[type_] = data
                redis.set('%s-%s' % (user_id, type_), data, redis_ex)

    # 返回最终数据
    return p_data


def ip_belong(ip, redis='127_0'):
    """
    根据IP定位地区
    :param ip:(type=str) IP地址
    :param redis:(type=str) 用于缓存地区结果的Redis，如为None则不使用缓存Redis，默认使用127_0
    :return result:(type=dict) code为地区的英文简称，name为中文全称
    """

    # 该函数的一些固定参数
    redis_ex = 1296000  # 缓存在Redis的时间，15d*24h*60m*60s=1296000s

    # 缓存Redis
    if redis is not None:
        redis = services.redis[redis]

    # 先尝试从Redis缓存拿结果
    ip_rk = 'ip@' + ip
    if redis is not None:
        result = redis.get(ip_rk)
        if result is not None:
            result = json.loads(result)
            return result

    # 获取结果
    code, name = '', ''
    if ip and len(ip) >= 7 and ip != '0.0.0.0':
        reader = geoip2_db.Reader(geoip2_path)
        try:
            response = reader.city(ip)
        except (geoip2_e.AddressNotFoundError, ValueError, TypeError):
            pass
        except Exception as e:
            services.logger.exception(e)
        else:
            code = response.country.iso_code  # 英文简称
            name = response.country.names.get('zh-CN', '')  # 中文全称
    result = {'code': code, 'name': name}

    # 缓存结果
    if redis is not None:
        redis.set(ip_rk, json.dumps(result), redis_ex)

    # 返回结果
    return result


def time_quantum(dt_format=None, is_date=False, start_offset=False):
    """
    根据脚本传参，返回开始与结束时间，所有时间均自动转化为整十分
    1.开始时间与结束时间均没有传入，则结束时间为当前时间，开始时间为上一个节点
    2.只传入开始时间，则结束时间为开始时间的下一个节点
    3.只传入结束时间，则开始时间为结束时间的上一个节点
    4.开始时间与结束时间均有传入，则使用传入时间
    5.若is_date为True，则不为以上规则，规则只有一条，就是开始和结束都默认为当天零点
    :param dt_format:(type=str) 返回格式化时间的格式，默认None则直接返回datetime对象
    :param is_date:(type=bool) 是否以日为单位，即使用第五点规则，默认False不使用
    :param start_offset:(type=bool) 入库明细时为防止有延迟数据，统计范围的开始时间需要往前推特定偏移量；is_date为True则该参数失效
    :return start_datetime:(type=datetime,str) 开始时间，根据dt_format返回datetime或str
    :return end_datetime:(type=datetime,str) 结束时间，同上
    :return start_dt_offset:(type=datetime,str) 当启用start_offset才会返回，根据偏移量得到的开始时间，其余同上
    """

    # 基础参数
    now_datetime = services.launch['datetime']
    argv = get_argv((pk_st, pk_et))
    start_time, end_time = argv[pk_st], argv[pk_et]
    offset_unit = -50  # 开始时间偏移量（单位：分钟）

    # 根据规则转化时间
    if not is_date:
        if start_time is None and end_time is None:
            end_datetime = datetime.strptime(now_datetime.strftime(format_datetime)[:-1] + '0', format_datetime)
            start_datetime = cf.datetime_timedelta('minutes', 0 - gc_interval, date_time=end_datetime)
        elif start_time is not None and end_time is None:
            start_datetime = datetime.strptime(start_time[:-1] + '0', format_datetime)
            end_datetime = cf.datetime_timedelta('minutes', gc_interval, date_time=start_datetime)
        elif start_time is None and end_time is not None:
            end_datetime = datetime.strptime(end_time[:-1] + '0', format_datetime)
            start_datetime = cf.datetime_timedelta('minutes', 0 - gc_interval, date_time=end_datetime)
        else:
            start_datetime = datetime.strptime(start_time[:-1] + '0', format_datetime)
            end_datetime = datetime.strptime(end_time[:-1] + '0', format_datetime)
    else:
        default_date = datetime.strptime(now_datetime.strftime(format_date), format_date)
        start_datetime = default_date if start_time is None else datetime.strptime(start_time, format_date)
        end_datetime = default_date if end_time is None else datetime.strptime(end_time, format_date)

    # 偏移开始时间
    if not is_date and start_offset:
        start_dt_offset = cf.datetime_timedelta('minutes', offset_unit, date_time=start_datetime)
    else:
        start_dt_offset = None

    # 返回结果
    if dt_format is not None:
        start_datetime, end_datetime = start_datetime.strftime(dt_format), end_datetime.strftime(dt_format)
        if start_dt_offset:
            start_dt_offset = start_dt_offset.strftime(dt_format)
    if not start_dt_offset:
        return start_datetime, end_datetime
    else:
        return start_datetime, end_datetime, start_dt_offset


def osa_server_dict(platform, game_code):
    """
    构造获取OSA配置的伺服器的字典，用于构造请求
    :param platform:(type=str) 哪个平台，晶绮jq、和悦hy、初心cx
    :param game_code:(type=str) 游戏代码
    :return request_dict:(type=dict) 用于构造内置请求对象所用参数的字典
    """

    # 采集方式
    way = 'db'

    # 数据库
    db_name = 'osa_%s' % platform

    # 表名
    table = 'gameservers'

    # 查询字段
    columns = ['servercode']

    # 查询条件
    after_table = 'WHERE gamecode="%s"' % game_code

    # 伺服器标识
    meta = 'server'

    # 构造字典并返回
    request_dict = {'way': way, 'db_name': db_name, 'table': table, 'columns': columns, 'after_table': after_table,
                    'meta': meta}
    return request_dict


def get_argv(argv_key, transform=False):
    """
    获取脚本传参
    :param argv_key:(type=str,list,tuple) 参数key，单个可直接传str，多个就传list或tuple（元素应为str）
    :param transform:(type=bool) 是否把没传参的key转换为空字符串，True转换，默认False不转换
    :return result:(type=str,None,dict) 传参结果，如argv_key为单个str则直接返回该结果，否则返回多个结果组成的dict
    """

    if isinstance(argv_key, str):
        result = getattr(services.argv, argv_key.replace('-', '_'))
        if transform and result is None:
            result = ''
    elif isinstance(argv_key, list) or isinstance(argv_key, tuple):
        result = dict()
        for one_key in argv_key:
            value = getattr(services.argv, one_key.replace('-', '_'))
            if transform and value is None:
                value = ''
            result[one_key] = value
    else:
        raise CheckUnPass('“argv_key”只能为str类型（单个）或list、tuple类型（多个）！')
    return result


def get_exchange_rate(expensive, cheap, month):
    """
    获取不同月份实时汇率
    :param expensive:(type=str) 相对价值高的货币
    :param cheap:(type=str) 相对价值低的货币
    :param month:(type=str) 实时汇率的月份，格式%Y-%m
    :return exchange_rate:(type=float) 当月汇率
    """

    # 数据库
    osa_db = services.mysql['osa_hy']  # 汇率数据
    cache_redis = services.redis['127_5']  # 缓存数据

    # 先从Redis缓存里查询
    expensive, cheap = expensive.lower(), cheap.lower()
    key = '%s_%s_%s' % (expensive, cheap, month.replace('-', ''))
    exchange_rate = cache_redis.get(key)

    # 没有就从OSA数据库获取
    if exchange_rate is None:
        after_table = 'WHERE from_cud="%s" AND to_cud="%s" AND month="%s"' % (expensive.upper(), cheap.upper(), month)
        db_result = osa_db.select('exchange_rate_month', columns=['rate'], after_table=after_table, fetchall=False)

        # 实时汇率还没更新或远古月份没有保存，标记为短时间缓存，并使用默认汇率
        # 已更新则使用新汇率，并长期缓存
        if db_result is None:
            short_cache = True
            exchange_rate = 30 if expensive == 'usd' and cheap == 'twd' else 1
        else:
            short_cache = False
            exchange_rate = db_result['rate']

        # 加入缓存并返回结果
        # 每月实时汇率长期缓存，短时间缓存就为半小时
        ex = None if not short_cache else 1800
        cache_redis.set(key, str(float(exchange_rate)), ex=ex)
    exchange_rate = float(exchange_rate)
    return exchange_rate


def get_secret(key):
    """
    获取secret的内容
    :param key:(type=str) 要获取secret的key
    :return result:(type=∞) secret内容
    """

    with open('%s/secret.json' % account_name, 'r') as f:
        result = cf.json_loads(f.read())[key]
    return result
