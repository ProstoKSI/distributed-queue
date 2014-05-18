"""Main module with DistributeQueue class"""

import logging
from time import sleep
import traceback

from distributed_queue import core, serializers, routers
from .backends import BackendConnectionError
from .utils import StoppableThread

logger = logging.getLogger('distributed_queue')


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


DEFAULT_BACKEND_SETTINGS_GROUP = 'default'


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
        for settings_group, backend_preferences in queue_settings.iteritems():
            if 'backend' not in backend_preferences:
                raise DistributedQueueError("`backend` is required option for backend settings")
            if 'queues' not in backend_preferences:
                raise DistributedQueueError("`queues` is required option for backend settings")

            router = backend_preferences.pop('router', None)
            if router is None:
                if 'default_queue' not in backend_preferences:
                    raise DistributedQueueError("`queues` is required option for backend settings")
                if backend_preferences['default_queue'] not in backend_preferences['queues']:
                    raise DistributedQueueError("`default_queue` should be listed in `queues`")
                router = routers.DefaultRouter(backend_preferences.pop('default_queue'))

            backend_settings = {
                'serializer': backend_preferences.pop('serializer', serializers.JsonSerializer()),
                'router': router,
                'queues': backend_preferences.pop('queues'),
            }
            backend = backend_preferences.pop('backend')
            if isinstance(backend, str):
                if backend in core.BACKEND_LIST:
                    backend = core.create_backend(backend, **backend_preferences)
                else:
                    raise DistributedQueueError("Unknown backend name `%s`. Expected: %s"\
                        % (backend, ','.join(core.BACKEND_LIST)))
            elif not (hasattr(backend, 'send') and hasattr(backend, 'receive')):
                raise DistributedQueueError("Backend is incompatible: " +\
                    "should have send and receive methods.")
            backend_settings['backend'] = backend
            self.backends[settings_group] = backend_settings

    # pylint: disable=R0913
    def send_custom(self, task, args, kwargs, backend_settings_group=None,
            queue_name=None, retries=1):
        """Send task to distributed queue, with serializing to string
        `.send_custom(
            'build_model',
            (arg1, arg2),
            {'kw1': 1, 'kw2': 2},
            queue_name='custom_queue',
            retries=0
        )`

        If `retries` is 0 than retry until success.
        """
        # Don't move this to default argument value as we want to accept None
        # as an indicator to use DEFAULT value.
        if backend_settings_group is None:
            backend_settings_group = DEFAULT_BACKEND_SETTINGS_GROUP
        backend_settings = self.backends[backend_settings_group]
        if queue_name is None:
            queue_name = backend_settings['router']\
                .get_queue_name(self.backends, task, args, kwargs)
        item = backend_settings['serializer'].dumps((task, args, kwargs))
        while 1:
            try:
                return backend_settings['backend'].send(queue_name, item)
            except BackendConnectionError:
                if retries > 0:
                    retries -= 1
                    if retries <= 0:
                        raise
                    sleep(1)

    def send(self, task, *args, **kwargs):
        """Send task shortcut for pythonic execution style.
        `.send('build_model', arg1, arg2, kw1=1, kw2=2)`
        """
        return self.send_custom(task, args, kwargs)

    def receive(self, backend_settings_group=None, queue_name=None, timeout=0):
        """Receive task from distributed queue, with deserializing from string

        If timeout is 0, then block indefinitely.
        """
        # Don't move this to default argument value as we want to accept None
        # as an indicator to use DEFAULT value.
        if backend_settings_group is None:
            backend_settings_group = DEFAULT_BACKEND_SETTINGS_GROUP
        backend_settings = self.backends[backend_settings_group]
        queue_name_list = [queue_name] if queue_name is not None else backend_settings['queues']
        task_id = None
        while 1:
            try:
                task_id, serialized_task = backend_settings['backend']\
                    .receive(queue_name_list, timeout=timeout)
            except BackendConnectionError:
                if timeout != 0:
                    timeout -= 1
                    if timeout <= 0:
                        break
                    sleep(1)
            else:
                break
        if task_id is None:
            return None
        return task_id, backend_settings['serializer'].loads(serialized_task)

    def keep_alive(self, task_id, backend_settings_group=None, queue_name=None):
        """Send keep-alive message and return False if the task has been canceled.
        """
        # Don't move this to default argument value as we want to accept None
        # as an indicator to use DEFAULT value.
        if backend_settings_group is None:
            backend_settings_group = DEFAULT_BACKEND_SETTINGS_GROUP
        backend_settings = self.backends[backend_settings_group]
        try:
            return backend_settings['backend'].keep_alive(task_id, queue_name=queue_name)
        except BackendConnectionError:
            pass
        # Assume that task is still marked alive if backend is unreachable
        # It's naive, but what can we do?
        return True

    def acknowledge(self, task_id, backend_settings_group=None,
            queue_name=None, retries=5):
        """Acknowledge task means that it was done and we want to mark it as
        done on the backend queue.
        """
        # Don't move this to default argument value as we want to accept None
        # as an indicator to use DEFAULT value.
        if backend_settings_group is None:
            backend_settings_group = DEFAULT_BACKEND_SETTINGS_GROUP
        backend_settings = self.backends[backend_settings_group]
        # pylint: disable=W0612
        for retries_left in xrange(retries, 0, -1):
            try:
                backend_settings['backend'].acknowledge(task_id, queue_name=queue_name)
            except BackendConnectionError:
                pass
            else:
                break

    def reject(self, task_id, backend_settings_group=None, queue_name=None):
        """Reject task means that we want to ignore/delete the task.
        """
        # Don't move this to default argument value as we want to accept None
        # as an indicator to use DEFAULT value.
        if backend_settings_group is None:
            backend_settings_group = DEFAULT_BACKEND_SETTINGS_GROUP
        backend_settings = self.backends[backend_settings_group]
        try:
            backend_settings['backend'].reject(task_id, queue_name=queue_name)
        except BackendConnectionError:
            pass

    def process(self, backend_settings_group=None):
        """Infinity task processing loop.
        """
        # Don't move this to default argument value as we want to accept None
        # as an indicator to use DEFAULT value.
        if backend_settings_group is None:
            backend_settings_group = DEFAULT_BACKEND_SETTINGS_GROUP
        while 1:
            try:
                received_data = self.receive(backend_settings_group=backend_settings_group)
                if received_data is not None:
                    # pylint: disable=W0633
                    task_id, (task, args, kwargs) = received_data
                    logger.debug("Received task '%s' with id = %s", task, task_id)
                    try:
                        register.process(task_id, task, args, kwargs)
                    except NotRegisteredError:
                        self.reject(task_id, backend_settings_group=backend_settings_group)
                    except Exception:
                        logger.exception("Unexpected error")
                    else:
                        logger.info("Task '%s' is finished", task)
                else:
                    sleep(1)
            except KeyboardInterrupt:
                logger.info("Keyboard interrupted")
                break
            except Exception:
                logger.exception("Unexpected error")


class Register(object):
    """Register class for storing task names with task processor functions.
    """

    CT_STARTED = 'started'
    CT_FINISHED = 'finished'
    CT_ERROR = 'error'
    CALLBACK_TYPE_LIST = [CT_STARTED, CT_FINISHED, CT_ERROR]

    registered_task_processors = {}
    registered_available_tasks = {}
    task_queue = None

    def __init__(self):
        def register_callback_type_decorator(callback_type):
            """Helper function to register user-friendly methods of callback
            registrator decorators.
            """
            self.__dict__['task_%s_callback' % callback_type] = lambda *args, **kwargs: \
                self._task_CALLBACK_TYPE_callback(callback_type, *args, **kwargs)

        for callback_type in self.CALLBACK_TYPE_LIST:
            register_callback_type_decorator(callback_type)

    def attach_task_queue(self, task_queue):
        """Attach 'task_queue' to registry as queue to use for sending outgoing tasks
        """
        self.task_queue = task_queue

    def register(self, task_uid, func):
        """Register local task processor.
        """
        if task_uid in self.registered_task_processors:
            raise RegisterError("Task processor with task_uid '%s' is already "
                "registered" % task_uid)
        if task_uid not in self.registered_available_tasks:
            raise RegisterError("Task processor with task_uid '%s' is not in "
                "available tasks. Register available tasks before registering "
                "processors.")
        self.registered_task_processors[task_uid] = func
        logger.info("Task processor for task '%s' is registered.", task_uid)

    def _send(self, task_settings, *args, **kwargs):
        """Send task to specified queue via specified backend
        """
        logger.debug("Sending task '%s'", task_settings['task_uid'])
        self.task_queue.send_custom(task_settings['task_uid'], args, kwargs,
            backend_settings_group=task_settings.get('backend_settings'),
            queue_name=task_settings.get('static_route_queue'))
        logger.debug("Task '%s' is sent", task_settings['task_uid'])

    def send(self, task_uid, *args, **kwargs):
        """Send task based on registry
        """
        if task_uid in self.registered_available_tasks:
            self._send(self.registered_available_tasks[task_uid], *args, **kwargs)
        else:
            logger.error("The task '%s' is not registered", task_uid)

    def _keep_alive(self, task_settings, task_id):
        """Send keep-alive message and return True if task has been canceled
        """
        logger.debug("Keeping alive task with id = %s", task_id)
        return self.task_queue.keep_alive(task_id,
            backend_settings_group=task_settings.get('backend_settings'),
            queue_name=task_settings.get('static_route_queue'))

    def _acknowledge(self, task_settings, task_id):
        """Send keep-alive message and return True if task has been canceled
        """
        logger.debug("Acknowledging task with id = %s", task_id)
        return self.task_queue.acknowledge(task_id,
            backend_settings_group=task_settings.get('backend_settings'),
            queue_name=task_settings.get('static_route_queue'))

    def register_available_task(self, func_name, task_settings=None):
        """Register available remote (or local) task to run it like
        `register.send_build_model(arg1, arg2)`
        """
        if task_settings is None:
            task_settings = {'task_uid': func_name}
        elif 'task_uid' not in task_settings:
            task_settings['task_uid'] = func_name
        task_uid = task_settings['task_uid']
        if task_uid in self.registered_available_tasks:
            raise RegisterError("Task with task_uid '%s' is already registered" % task_uid)
        self.registered_available_tasks[task_uid] = task_settings
        if func_name is not None:
            self.__dict__['send_' + func_name] = lambda *args, **kwargs: \
                self._send(task_settings, *args, **kwargs)
        logger.info("Task %s is available now.",
            "'%s' (%s)" % (task_uid, func_name) if func_name else "'%s'" % task_uid)
        if 'callbacks_queue_settings' in task_settings:
            for callback_type in self.CALLBACK_TYPE_LIST:
                try:
                    self.register_available_task(None, task_settings={
                        'task_uid': '%s:%s' % (task_uid, callback_type),
                        'backend_settings': task_settings['callbacks_queue_settings'],
                    })
                except RegisterError:
                    logger.debug("Callback '%s' couldn't be registered for "
                        "task uid '%s'. Exception:\n%s",
                            callback_type, task_uid, traceback.format_exc())

    def register_available_tasks(self, available_tasks):
        """Register available remote tasks. We need only task id to get
        started, although you can specify backend settings group and
        queue_name.

        For example:

        T_BUILD_MODEL = 'build_model'
        register.register_available_tasks([T_BUILD_MODEL])

        T_PROCESS_NEW_DATASOURCE = 1
        T_TRANSFORM_DATASET = 2
        available_tasks = {
            'process_new_datasource': {
                # you can specify custom task id, i.e. number constant
                'task_uid': T_PROCESS_NEW_DATASOURCE,
            }
            'make_dataset': {
                'task_uid': T_TRANSFORM_DATASET,
                'backend_settings': 'default',
                'callbacks_queue_settings': 'hadoop_results',

                # you can even specify the queue_name to apply static routing
                'static_route_queue': 'hadoop_workers',
            }
        }
        register.register_available_tasks(available_tasks)

        ...

        register.send(T_PROCESS_NEW_DATASOURCE, link)
        register.send_make_dataset(link)
        register.send_build_model(settings)
        """
        if isinstance(available_tasks, list):
            available_tasks = {name: {'task_uid': name} for name in available_tasks}
        for func_name, task_settings in available_tasks.iteritems():
            self.register_available_task(func_name, task_settings)

    def process(self, task_id, task, args, kwargs):
        """Process received task.
        """
        if task not in self.registered_task_processors:
            raise NotRegisteredError
        kwargs['dq_task_settings'] = self.registered_available_tasks[task]
        kwargs['dq_task_id'] = task_id
        self.registered_task_processors[task](*args, **kwargs)

    # pylint: disable=R0912
    def task(self, task_uid=None, ignore_result=False, preserve_args=None):
        """This is a decorator, which registers a decorated function as a task.
        It will not change the function behaviour if it executes directly.

        `task_uid` is a unique task function identifier, which is synced
        between master and workers. It's suggested to use integers, but you can
        use strings as well.

        `ignore_results` is a boolean flag than is used to determine whether or
        not task status callbacks ought to be sent.

        `preserver_args` is a list of arguments that task will receive and will
        send back with status callbacks.
        """
        if isinstance(task_uid, int):
            _task_str_uid = unicode(task_uid)
        else:
            _task_str_uid = task_uid
        task_started = _task_str_uid + ':' + self.CT_STARTED
        task_finished = _task_str_uid + ':' + self.CT_FINISHED
        task_error = _task_str_uid + ':' + self.CT_ERROR

        def wrapper(func):
            """Wrapper that registers the task.
            """
            def wrapped_func(*args, **kwargs):
                """Wrapped function that will be called by register
                when an appropriate task is received
                """
                task_settings = kwargs.pop('dq_task_settings')
                task_id = kwargs.pop('dq_task_id')
                if not ignore_result:
                    preserved_args = {arg: kwargs[arg] for arg in preserve_args}
                    register.send(task_started, **preserved_args)
                try:
                    # Run user's function asyncronously, use thread to do that.
                    func_async = StoppableThread(target=func, args=args, kwargs=kwargs)
                    func_async.start()

                    # Send keep-alive and monitor whether task has not been
                    # canceled yet.
                    keep_alive_helper = 0
                    is_stopped = False
                    while func_async.is_alive():
                        sleep(0.1)
                        keep_alive_helper += 1
                        # Keep-alive every (0.1 * 200) == 20 seconds
                        if keep_alive_helper == 200:
                            if not self._keep_alive(task_settings, task_id):
                                is_stopped = True
                                func_async.stop()
                            keep_alive_helper = 0

                    if is_stopped:
                        logger.info("Task with id = %s was stopped.", task_id)

                    func_result = func_async.get_result()
                    if isinstance(func_result, str):
                        logger.error("Exception was raised while calling "
                            "the task '%s'.\n%s", task_uid, func_result)
                    if not ignore_result:
                        if isinstance(func_result, str):
                            register.send(task_error, exception=func_result,
                                **preserved_args)
                        else:
                            register.send(task_finished, _is_stopped=is_stopped,
                                **dict(preserved_args, **func_result))

                    self._acknowledge(task_settings, task_id)
                except Exception: # pylint: disable=W0703
                    logger.exception("Exception was raised while calling the task '%s'.", task_uid)
                    if not ignore_result:
                        register.send(task_error, exception=traceback.format_exc(),
                            **preserved_args)

            register.register(task_uid, wrapped_func)
            return func
        return wrapper

    def _task_CALLBACK_TYPE_callback(self, callback_type, task_uid):
        """Template of callback function decorator.
        """
        if isinstance(task_uid, int):
            _task_str_uid = unicode(task_uid)
        else:
            _task_str_uid = task_uid

        callback_task_uid = _task_str_uid + ':' + callback_type
        return self.task(task_uid=callback_task_uid, ignore_result=True)


register = Register()
