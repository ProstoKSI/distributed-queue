"""This module contains dummy backend for distributed queue.
Implementation of dummy backend is based on local queue.
Use it only for testing purposes - it's not distributed.
"""

# pylint: disable=F0401,E0611
from six.moves.queue import Queue

from distributed_queue.backends import BaseBackend


class DummyBackend(BaseBackend):
    """DummyBackend for DistributedQueue for local testing.
    Uses local queue internally.
    """

    def __init__(self):
        """Create DummyBackend"""
        self.queues = {}
        self.last_task_id = 0

    def send(self, queue_name, item):
        """Push element into local queue"""
        if queue_name not in self.queues:
            self.queues[queue_name] = Queue()
        self.last_task_id += 1
        self.queues[queue_name].put((self.last_task_id, item))

    def receive(self, queue_name_list, timeout=0):
        """Pop element from local queue.

        Limitations:
        - Elements pop immidiatelly, there is no waiting interval;
        - Timeout is ignored.
        """
        for queue_name in queue_name_list:
            if queue_name in self.queues:
                return self.queues[queue_name].get()
        return None

