# encoding: utf-8
"""Useful utils for distributed queue will be here.
"""

import Queue
import threading
import traceback


class StoppableThread(threading.Thread):
    """This is thread can be stopped.

    Note: Thread by default does not return function result in any case,
    which is why I've implemented this workaroung with built-in Queue.
    """
    def __init__(self, **kwargs):
        super(StoppableThread, self).__init__(**kwargs)
        self.__target = kwargs.get('target')
        self.__args = kwargs.get('args')
        if self.__args is None:
            self.__args = ()
        self.__kwargs = kwargs.get('kwargs')
        if self.__kwargs is None:
            self.__kwargs = {}
        self.__result_queue = Queue.Queue()
        self.__stopped = threading.Event()

    def stop(self):
        """Stop the thread. It will not terminate code, but set the flag that
        should be handled in executed function.
        """
        self.__stopped.set()

    def is_stopped(self):
        """Check the status of the thread. It only monitors the flag state. If
        task is stopped you have to pay attention to `.is_alive()`.
        """
        return self.__stopped.is_set()

    def run(self):
        """Run the target function, check expected result and propagate
        exceptions.
        """
        try:
            self.__kwargs['_is_stopped'] = self.__stopped.is_set
            try:
                if self.__target:
                    func_result = self.__target(*self.__args, **self.__kwargs)
            finally:
                # Avoid a refcycle if the thread is running a function with
                # an argument that has a member that points to the thread.
                del self.__target, self.__args, self.__kwargs
            if func_result is None:
                func_result = {}
            elif not isinstance(func_result, dict):
                raise TypeError("Task has to return a dict or None.")
        except Exception: # pylint: disable=W0703
            self.__result_queue.put(traceback.format_exc())
        else:
            self.__result_queue.put(func_result)

    def get_result(self):
        """Return results of target function execution.
        """
        self.join()
        try:
            return self.__result_queue.get_nowait()
        except Queue.Empty:
            return None

