"""Distributed Queue module"""
from .version import __version__, VERSION
from .queue import register

PROJECT = __project__ = "distributed_queue"

def setup_dq_environment(queues_settings, tasks):
    """Initializes queues and registers all available tasks.
    """
    from .queue import DistributedQueue
    DistributedQueue(queues_settings)
    register.register_available_tasks(tasks)
