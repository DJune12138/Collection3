"""
调度器组件：
1.缓存请求对象，并为下载器提供请求对象，实现请求的调度
"""

from six.moves.queue import Queue, Empty


class Scheduler(object):
    """
    调度器组件
    """

    def __init__(self):
        self.__queue = Queue()

    def add_request(self, request):
        """
        添加请求对象
        :param request:(type=Request) 初始请求对象
        """

        self.__queue.put(request)

    def get_request(self):
        """
        获取一个请求对象并返回
        :return request:(type=Request,None) 从Queue获取的请求对象，为空时返回None
        """

        try:
            request = self.__queue.get(block=False)  # 设置为非阻塞
        except Empty:  # 获取为空会抛异常，返回None
            return None
        else:
            return request
