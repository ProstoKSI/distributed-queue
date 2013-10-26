"""Main module with DistributeQueue class"""

import logging
from time import sleep
import traceback

from distributed_queue import core, serializers, routers
from .backends import BackendConnectionError


class DistributedQueueError(Exception):
    """DistributedQueueError - base class for all errors in this queue
    """
    pass


class RegisterError(Exception):
    """Exception that is used to indicate configuration error in Register.
    """
    pass


class NotRegisteredError(Exception):
    """Exception that is used to indicate, that requested task is not registered yet.
    """
    pass


class DistributedQueue(object):
    """DistributedQueue - inteface for distributed queues
    """
    
    def __init__(self, queue_settings):
        """Create DistributedQueue by specifying queue settings.
        QUEUES_SETTINGS = {
            'default': {
                'backend': 'redis', # required, now available dummy/redis
                'serializer': CustomSerializer(), # optional, specify custom serializer
                                                  # instead of JSON default
                'router': CustomRouter(), # optional, specify custom router
                                          # instead of dummy returning constant `default_queue`
                'queues': ['tasks', ], # backend-specific, but required for all currently
                                       # available backends
                'default_queue': 'tasks', # required for default router `DefaultRouter`
                'host': 'localhost', # backend-specific
                'port': 6379, # backend-specific
                'database': 0, # backend-specific
            }
        }
        """
        register.attach_task_queue(self)
        self.backends = {}
        for settings_group, backend_settings in queue_settings.items():
            if 'backend' not in backend_settings:
                raise DistributedQueueError("`backend` is required option for backend settings")
            if 'queues' not in backend_settings:
                raise DistributedQueueError("`queues` is required option for backend settings")

            router = backend_settings.pop('router', None)
            if router is None:
                if 'default_queue' not in backend_settings:
                    raise DistributedQueueError("`queues` is required option for backend settings")
                if backend_settings['default_queue'] not in backend_settings['queues']:
                    raise DistributedQueueError("`default_queue` should be listed in `queues`")
                
            backend_group = {
                'serializer': backend_settings.pop('serializer', serializers.JsonSerializer()),
                'router': router if router is not None else 
                    routers.DefaultRouter(backend_settings.pop('default_queue')),
                'queues': backend_settings.pop('queues'),
            }
            backend = backend_settings.pop('backend')
            if isinstance(backend, str):
                if backend in core.BACKEND_LIST:
                    backend = core.create_backend(backend, **backend_settings)
                else:
                    raise DistributedQueueError("Unknown backend name `%s`. Expected: %s"\
                        % (backend, ','.join(core.BACKEND_LIST)))
            elif not (hasattr(backend, 'send') and hasattr(backend, 'receive')):
                raise DistributedQueueError("Backend is incompatible: " +\
                    "should have send and receive methods.")
            backend_group['backend'] = backend
            self.backends[settings_group] = backend_group

    def send_custom(self, task, args, kwargs, backend_group=None, queue_name=None, retries=1):
        """Send task to distributed queue, with serializing to string
        `.send('build_model', (arg1, arg2), {'kw1': 1, 'kw2': 2}, queue_name='custom_queue', retries=0)`

        If `retries` is 0 than retry until success.
        """
        if backend_group is None:
            backend_group = 'default'
        backend_group_info = self.backends[backend_group]
        if queue_name is None:
            queue_name = backend_group_info['router'].get_queue_name(self.backends, task, args, kwargs)
        if retries <= 0:
            retries = -1
        item = backend_group_info['serializer'].dumps((task, args, kwargs))
        while 1:
            try:
                backend_group_info['backend'].send(queue_name, item)
            except BackendConnectionError:
                if retries != -1:
                    retries -= 1
                    if retries < 0:
                        raise
                    sleep(1)
            else:
                break

    def send(self, task, *args, **kwargs):
        """Send task shortcut for more pythonic execution style.
        `.p('build_model', arg1, arg2, kw1=1, kw2=2)`
        """
        self.send_custom(task, args, kwargs)

    def receive(self, backend_group=None, queue_name=None, timeout=0):
        """Receive task from distributed queue, with deserializing from string
        
        If timeout is 0, then block indefinitely.
        """
        if backend_group is None:
            backend_group = 'default'
        backend_group = self.backends[backend_group]
        queue_name_list = [queue_name] if queue_name is not None else backend_group['queues']
        item = None
        while 1:
            try:
                item = backend_group['backend'].receive(queue_name_list, timeout=timeout)
            except BackendConnectionError:
                if timeout != 0:
                    timeout -= 1
                    if timeout <= 0:
                        break
                    sleep(1)
            else:
                break
        if item is None: 
            return None
        return backend_group['serializer'].loads(item)

    def process(self):
        """Infinity task processing loop.
        """
        while 1:
            try:
                received_data = self.receive()
                if received_data is not None:
                    task, args, kwargs = received_data
                    try:
                        register.process(task, args, kwargs)
                    except NotRegisteredError:
                        logging.warning("Received unexpected task '%s'" % task)
                else:
                    sleep(1)
            except KeyboardInterrupt:
                logging.warning("Keyboard interrupted")
                break
            except Exception:
                logging.exception("Exception occured while processing received task.")


class Register(object):
    """Register class for storing task names with task processor functions.
    """

    HS_STARTED = 'started'
    HS_FINISHED = 'finished'
    HS_ERROR = 'error'
    HANDLER_STATUS_LIST = [HS_STARTED, HS_FINISHED, HS_ERROR]
    
    registered_task_processors = {}
    registered_available_tasks = {}
    task_queue = None
    
    def attach_task_queue(self, task_queue):
        """Attach 'task_queue' to registry as queue to use for sending outgoing tasks"""
        self.task_queue = task_queue

    def register(self, name, func):
        """Register local task processor.
        """
        if name in self.registered_task_processors:
            raise RegisterError("Task processor with name '%s' is already registered" % name)
        self.registered_task_processors[name] = func
        self.register_available_task(name)

    def _send(self, task_settings, *args, **kwargs):
        """Send task to specified queue via specified backend"""
        self.task_queue.send_custom(task_settings['task'], args, kwargs,
            backend_group=task_settings.get('backend_group'),
            queue_name=task_settings.get('queue_name'))

    def send(self, task, *args, **kwargs):
        """Send task based on registry"""
        if task in self.registered_available_tasks:
            self._send(self.registered_available_tasks[task], *args, **kwargs)
        else:
            logging.warning("Failed to send task '%s' - is not in registry", task)

    def register_available_task(self, func_name, task_settings=None):
        """Register available remote (or local) task to run it like
        `register.send_build_model(arg1, arg2)`
        """
        if task_settings is None:
            task_settings = {'task': func_name}
        elif 'task' not in task_settings:
            task_settings['task'] = func_name
        self.registered_available_tasks[func_name] = task_settings
        self.__dict__['send_' + func_name] = lambda *args, **kwargs: \
            self._send(task_settings, *args, **kwargs)

    def register_available_tasks(self, available_tasks):
        """Register available remote tasks, that we have only 'headers'.
        For example:

        T_UPLOAD_DATASOURCE = 'process_upload_datasource'
        T_TRANSFORM_DATASET = 'make_dataset'
        T_BUILD_MODEL = 'build_model'
        available_tasks = {
            T_UPLOAD_DATASOURCE: {
                'task': 'upload_datasource', # you can specify custom task name
                                             # for 'function name', see example bellow
            }
            T_TRANSFORM_DATASET: {
                'task': 'transform_dataset',
                'backend_group': 'default',
                'queue_name': 'hadoop_workers', # even you can specify queue_name, to avoid routing
            }
        }
        register.register_available_tasks(available_tasks)
        register.register_available_tasks([T_BUILD_MODEL])

        ...

        register.send(T_UPLOAD_DATASOURCE, link)
        register.send_make_dataset(link)
        register.send_build_model(settings)
        """
        if isinstance(available_tasks, list):
            available_tasks = {name: {'task': name} for name in available_tasks}
        for func_name, task_settings in available_tasks.iteritems():
            self.register_available_task(func_name, task_settings)

    def process(self, task, args, kwargs):
        """Process received task.
        """
        if task not in self.registered_task_processors:
            raise NotRegisteredError
        self.registered_task_processors[task](*args, **kwargs)

    def task(self, results=None, name=None):
        """Task decorator, that allows direct function execution while it's
        already register the task. If `name` not passed - function's name will be used
        `results` specifies queue or (backend, queue) to send started/finished/error tasks to
        """
        def wrapper(func):
            """Wrapper, that register task."""
            task_name = func.__name__ if name is None else name
            def wrapped_func(*args, **kwargs):
                """Wrapped function that will be called by register 
                when appropriate task is received
                """
                if results is not None:
                    unique_id = kwargs.pop('unique_id', None)
                    if unique_id is None: 
                        logging.warning("Received task '%s' without unqiue_id argument", task_name)
                        return
                    register.send(task_name + '_' + self.HS_STARTED, unique_id=unique_id)
                try:
                    result = func(*args, **kwargs)
                    if results is not None:
                        register.send(task_name + '_' + self.HS_FINISHED,
                            unique_id=unique_id, **result)
                except Exception as exp:
                    logging.exception("Error when calling task '%s'.", task_name)
                    if results is not None:
                        register.send(task_name + '_' + self.HS_ERROR,
                            unique_id=unique_id, exception=traceback.format_exc())
            register.register(task_name, wrapped_func)
            if results is not None:
                task_settings = {}
                if isinstance(results, str):
                    backend_group = 'default'
                    queue_name = results
                elif isinstance(results, tuple):
                    backend_group = results[0]
                    queue_name = results[1]
                else:
                    raise RegisterError("Incorrect argument to Register.task decorator."
                        " 'results' can only be queue_name or (backend, queue_name)")
                for handler_status in self.HANDLER_STATUS_LIST:
                    register.register_available_task(task_name + '_' + handler_status, \
                        {'backend_group': backend_group, 'queue_name': queue_name})
            return func
        return wrapper


register = Register()

