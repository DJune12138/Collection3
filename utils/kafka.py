"""
连接kafka

关于消费者consumer的使用：
1、获取对象后，直接for遍历
2、每次for出来的对象，它的value属性即为内容
"""

from kafka import KafkaConsumer  # pip3 install kafka-python


class Kafka(object):
    """
    kafka
    """

    def __init__(self, topic, group_id, bootstrap_servers, consumer_timeout_ms=float('inf')):
        """
        初始配置
        :param topic:(type=str) 连接的topic
        :param group_id:(type=str) 连接的group_id
        :param bootstrap_servers:(type=str,list) 连接的服务器，可str单个，可list多个，参考格式“ip:端口”
        :param consumer_timeout_ms:(type=int,float) 阻塞等待新订阅时间（单位毫秒，1秒=1000毫秒），默认正无穷
        """

        # 连接信息
        self.topic = topic
        self.group_id = group_id
        self.bootstrap_servers = bootstrap_servers
        self.consumer_timeout_ms = consumer_timeout_ms

        # 消费者对象
        self.con = None

    @property
    def consumer(self):
        """
        返回消费者对象
        :return consumer:(type=KafkaConsumer) 消费者对象
        """

        if self.con is None:
            self.con = KafkaConsumer(self.topic, group_id=self.group_id, bootstrap_servers=self.bootstrap_servers,
                                     consumer_timeout_ms=self.consumer_timeout_ms)
        consumer = self.con
        return consumer
