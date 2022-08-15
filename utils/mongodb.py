"""
连接MongoDB

1.MongoDB实例为一个现成的连接池，线程安全
"""

import pymongo


class MongoDB(object):
    """
    MongoDB
    """

    def __init__(self, host='127.0.0.1', port=27017, **kwargs):
        """
        初始配置
        :param host:(type=str) 连接主机的ip地址，默认本地
        :param port:(type=int) 连接主机的端口，默认27017
        :param kwargs:(type=dict) 其余的关键字参数，用户名为username，密码为password
        """

        # 连接信息
        self.host = host
        self.port = port
        self.init_kwargs = kwargs

        # 连接对象，初始化时为None
        self.connect = None

    @property
    def client(self):
        """
        返回连接对象
        1.如要获取数据库或集合对象，可采用字典键值的形式，例如client["db"]["collection"]
        2.其余操作参考：https://blog.csdn.net/liujingliuxingjiang/article/details/122069240
        :return client:(type=MongoClient) 连接对象
        """

        if self.connect is None:
            self.connect = pymongo.MongoClient(host=self.host, port=self.port, **self.init_kwargs)
        client = self.connect
        return client


def mongodb_operation(pymongo_object, *args):
    """
    对pymongo对象执行指令，获取结果
    :param pymongo_object:(type=MongoClient,Database,Collection) 要执行操作的pymongo对象
    :param args:(type=tuple) 指令列表，需要按顺序，实例：find().limit(3)，传入["find"],["limit",3]
    :return result:(type=Cursor,dict) 指令执行后返回的结果，如果是多条结果则返回可遍历pymongo对象Cursor，单条则为dict
    """

    result = pymongo_object  # 初始化使用pymongo对象作为result
    for one in args:
        key = one[0]
        if len(one) > 1:
            result = getattr(result, key)(*one[1:])
        else:
            result = getattr(result, key)()
    return result


if __name__ == '__main__':
    from datetime import datetime

    c = {
        "host": "43.129.188.246",
        "port": 27017
    }
    aaa = MongoDB(**c).client['LogDB_DWC']['majiang_table_log']
    # print(aaa)
    # {'$gte': datetime.strptime('2022-08-12', '%Y-%m-%d')}
    # r = aaa.find().limit(3)
    # for i in r:
    #     print(i)
    # print(r)
    r = aaa.find_one()
    print(r)

    # r = get_result(aaa, ['find'], ['limit', 3])
    # r = mg_result(aaa, ['find', {'EndTime': {'$gte': datetime.strptime('2022-08-12', '%Y-%m-%d')}}], ['limit', 2])
    # for i in r:
    #     print(i)
    # print(r)
