"""
业务公共函数
"""

import os
import json
from geoip2 import database as geoip2_db, errors as geoip2_e
import services  # 该模块在加载服务前就已经被导入，只能导入总模块，否则所有服务都会是加载前的None
from config import ding_token, factory_config, factory_code, pegging_redis, geoip2_path
from utils import common_function as cf


def send_ding(msg, e, group):
    """
    发送钉钉消息
    :param msg:(type=str) 要发送的钉钉信息内容
    :param e:(type=Exception) 报错对象
    :param group:(type=str) 要发送的钉钉群组
    :return result:(type=bool) 发送结果，成功为True，失败为False
    """

    # 校验group
    group_list = ding_token.keys()
    if group not in group_list:
        raise ValueError('group只能为%s其中一个！' % '、'.join(group_list))

    # 钉钉消息内容
    log_path = factory_config[factory_code]['logger_config.log_path']
    log_path = log_path if log_path is not None else os.path.join(os.getcwd(), 'log')
    ding_msg = '任务出错了！详情请查看报错日志！\n执行任务的主机IP：%s\n报错日志路径：%s\nMsg：%s\nError：%s' \
               % (services.launch['ip'], log_path, msg, str(e))

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


def game_money_dict(platform, game_code, start, end, game_point=False):
    """
    构造获取平台储值的字典，用于构造请求
    :param platform:(type=str) 哪个平台，晶绮jq、和悦hy、初心cx
    :param game_code:(type=str) 游戏代码
    :param start:(type=str) 查询时间段的开始时间
    :param end:(type=str) 查询时间段的结束时间
    :param game_point:(type=bool) 使用“gamepoint>0”的条件，默认不使用
    :return request_dict:(type=dict) 用于构造内置请求对象所用参数的字典
    """

    # 采集方式
    way = 'db'

    # 数据库
    db_name = '%s_realtime' % platform

    # 表名
    table = 'game_money'

    # 查询字段
    columns = ['orderid', 'servercode', 'gamepoint', 'userid', 'create_time', 'comefrom']

    # 查询条件
    game_point = ' AND gamepoint>0' if game_point else ''
    after_table = 'WHERE gamecode="%s" AND (create_time BETWEEN "%s" AND "%s") AND status IN (1,10,20)%s' % (
        game_code, start, end, game_point)

    # 储值标识
    meta = 'pay'

    # 构造字典并返回
    request_dict = {'way': way, 'db_name': db_name, 'table': table, 'columns': columns, 'after_table': after_table,
                    'meta': meta}
    return request_dict


def u_pegging(platform, game_code, userid_list, type_list, pass_redis=True):
    """
    根据userid反查数据，比如IP地址、系统
    :param platform:(type=str) 哪个平台，晶绮jq、和悦hy、初心cx
    :param game_code:(type=str) 游戏代码
    :param userid_list:(type=list,set) 要反查的userid列表
    :param type_list:(type=list) 反查类型的列表，os为系统，ip为IP地址
    :param pass_redis:(type=bool) 是否跳过查Redis，直接查MySQL，默认True
    :return p_data:(type=dict) 反查的结果
    """

    # 该函数的一些固定参数
    column_map = {'os': 'comefrom', 'ip': 'ipaddr'}  # MySQL字段映射
    default = {'os': 'Android'}  # 默认值
    redis_ex = 259200  # 缓存在Redis的时间，3d*24h*60m*60s=259200s
    mysql_max = 100  # MySQL每批反查数量

    # 数据库连接
    mysql = services.mysql['%s_realtime' % platform]
    redis = services.redis[pegging_redis]

    # 最终的数据集合
    p_data = dict()

    # 1.从Redis根据puid查
    none = set()  # Redis没查出来的puid集合
    for userid in userid_list:
        userid = str(userid)
        for type_ in type_list:
            p_data.setdefault(userid, dict())[type_] = default.get(type_, '')  # 把默认值赋予给数据集合
            if not pass_redis:
                data = redis.get(cf.calculate_fp([game_code, userid, type_]))  # 从Redis查
                if data is None:  # 查不出来就记录
                    none.add(userid)
                else:  # 查得出来就覆盖数据集合
                    p_data.setdefault(userid, dict())[type_] = data
            else:
                none.add(userid)

    # 2.Redis查不出的再从MySQL分批查
    none = list(none)
    len_ = len(none)
    for i in range(int(len_ / mysql_max) if not len_ % mysql_max else int(len_ / mysql_max) + 1):
        sql_data = mysql.select('game_user', columns=['userid'] + [column_map[type_] for type_ in type_list],
                                after_table='WHERE gamecode="%s" AND userid IN (%s)' % (
                                    game_code, ','.join(none[i * mysql_max: i * mysql_max + mysql_max])))
        for one_data in sql_data:
            userid = str(one_data['userid'])
            for type_ in type_list:
                data = one_data[column_map[type_]]
                if type_ == 'os':  # 设备还需要转换一下写法
                    if data == 'android':
                        data = 'Android'
                    elif data == 'ios':
                        data = 'IOS'
                p_data.setdefault(userid, dict())[type_] = data
                redis.set(cf.calculate_fp([game_code, userid, type_]), data, redis_ex)

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
    code = 'unknow'
    name = 'unknow'
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
            name = response.country.names['zh-CN']  # 中文全称
    result = {'code': code, 'name': name}

    # 缓存结果
    if redis is not None:
        redis.set(ip_rk, json.dumps(result), redis_ex)

    # 返回结果
    return result
