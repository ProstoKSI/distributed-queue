"""Module with different task routers"""


class BaseRouter(object):
    """Base class for task routing"""

    def get_queue_name(self, queue_settings, task, args, kwargs):
        """Return queue_name by given queue settings, task name, its args and kwargs."""
        raise NotImplementedError()


class DefaultRouter(BaseRouter):
    """The simpliest task routing, just return constant"""

    def __init__(self, default_queue_name):
        self.default_queue_name = default_queue_name

    def get_queue_name(self, queue_settings, task, args, kwargs):
        return self.default_queue_name
