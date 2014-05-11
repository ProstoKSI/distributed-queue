"""Module defined BaseBackend class
"""

class BackendConnectionError(Exception):
    pass


class BaseBackend(object):
    """Base class for all backends"""
    
    def send(self, queue_name, item):
        """Virtual method for putting element to queue"""
        raise NotImplementedError()

    def receive(self, queue_name_list):
        """Virtual method for getting element from queue.
        Note: receive() should never lose a task!
        """
        raise NotImplementedError()

