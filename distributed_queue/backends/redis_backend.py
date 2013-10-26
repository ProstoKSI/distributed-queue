"""Redis backend module for Distributed Queue
"""

import redis

from distributed_queue.backends import BaseBackend, BackendConnectionError

class RedisBackend(BaseBackend):
    """RedisBackend - backend for distributed queue that stores queue in redis
    """

    BACKEND_NAME = 'redis'

    def __init__(self, host='localhost', port=6379, database=0):
        """Create RedisBackend, set:
            `host` where redis resides (default: localhost)
        """
        self.queue = redis.StrictRedis(host=host, port=port, db=database)

    def send(self, queue_name, item):
        """Enqueue element to specified queue in Redis"""
        try:
            self.queue.rpush(queue_name, item)
        except redis.ConnectionError:
            raise BackendConnectionError

    def receive(self, queue_name_list, timeout=0):
        """Dequeue element from Redis on of queue from passed list.
        
        If timeout is 0, then block indefinitely.
        """
        try:
            _, item = self.queue.blpop(queue_name_list, timeout=timeout)
        except redis.ConnectionError:
            raise BackendConnectionError
        return item

