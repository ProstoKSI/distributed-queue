"""Redis backend module for Distributed Queue
"""

import logging
import redis
import time

from distributed_queue.backends import BaseBackend, BackendConnectionError

logger = logging.getLogger('distributed_queue')


class RedisBackend(BaseBackend):
    """RedisBackend - backend for distributed queue that stores queue in redis
    """

    BACKEND_NAME = 'redis'

    LOCK_SUFFIX = ':locked'
    QUEUE_KEY_PREFIX = 'dq:q:'
    RECEIVED_TASKS_QUEUE_SUFFIX = ':received'
    RECEIVED_TASKS_RESTORE_INTERVAL = 60
    RECEIVED_TASKS_RESTORE_COUNT_LIMIT = 1000
    POP_TASK_RETRIES_COUNT = 5
    TASK_KEY_PREFIX = 'dq:t:'
    LAST_TASK_ID_KEY = 'dq:t::last_id'
    TASK_LOCK_TIMEOUT = 60

    # Note: Lua functions runs atomicaly in Redis
    # Safely pop a task, which means usage of RPOPLPUSH to temp list and
    # restore outdated tasks to the main queue to run them again.
    REDIS_LUA_POP_TASK_FUNCTION_TEMPLATE = '''
        local new_tasks_queue_name = ARGV[1]
        local received_tasks_queue_name = ARGV[1]..'%(received_tasks_queue_suffix)s'

        for i=1,%(retries_count)d do
            local task_id = redis.call('RPOPLPUSH', new_tasks_queue_name, received_tasks_queue_name)
            if task_id then
                local lock_task_key = '%(task_key_prefix)s'..task_id..'%(lock_suffix)s'
                local is_lock_succeeded = redis.call(
                    'SET', lock_task_key, '%(server_name)s',
                    'EX', %(lock_timeout)d, 'NX')
                if is_lock_succeeded then
                    local task = redis.call('GET', '%(task_key_prefix)s'..task_id)
                    if task then
                        return {task_id, task}
                    else
                        redis.call('LPOP', received_tasks_queue_name)
                        redis.call('DEL', lock_task_key)
                    end
                end
            else
                break
            end
        end'''

    # Restore or delete tasks when we scan backup list in Redis
    REDIS_LUA_RESTORE_TASKS_FUNCTION_TEMPLATE = '''
        local new_tasks_queue_name = ARGV[1]
        local received_tasks_queue_name = ARGV[1]..'%(received_tasks_queue_suffix)s'
        local tasks_restores_limit = math.min(
            %(tasks_restores_limit)d,
            redis.call('LLEN', received_tasks_queue_name)
        )
        for i=1,tasks_restores_limit do
            local task_id = redis.call('LINDEX', received_tasks_queue_name, -1)
            if not task_id then
                break
            end
            local task_exists = redis.call('EXISTS', '%(task_key_prefix)s'..task_id)
            if task_exists == 1 then
                local task_is_locked = redis.call('EXISTS', '%(task_key_prefix)s'..task_id..'%(lock_suffix)s')
                if task_is_locked == 1 then
                    redis.call('RPOPLPUSH', received_tasks_queue_name, received_tasks_queue_name)
                else
                    redis.call('RPOPLPUSH', received_tasks_queue_name, new_tasks_queue_name)
                end
            else
                redis.call('LTRIM', received_tasks_queue_name, 0, -2)
            end
        end'''

    def __init__(self, host='localhost', port=6379, database=0, worker_uid=None):
        """Create RedisBackend, set:
            `host` where Redis resides (default: localhost);
            `port` is Redis' listening port (default: 6379);
            `database` is Redis' database id, there is no names for Redis
                databases, it has to be integer value;
            `worker_uid` is unique server name, is used only as locking value
                that can help with monitoring of distributed queue process;
        """
        if worker_uid is None:
            raise ValueError("worker_uid should not be None.")
        self.worker_uid = worker_uid
        self.queue = redis.StrictRedis(host=host, port=port, db=database)
        self._lua_pop_task = self.queue.register_script(
            self.REDIS_LUA_POP_TASK_FUNCTION_TEMPLATE \
                 % {
                    'received_tasks_queue_suffix': self.RECEIVED_TASKS_QUEUE_SUFFIX,
                    'retries_count': self.POP_TASK_RETRIES_COUNT,
                    'task_key_prefix': self.TASK_KEY_PREFIX,
                    'lock_suffix': self.LOCK_SUFFIX,
                    'lock_timeout': self.TASK_LOCK_TIMEOUT,
                    'server_name': self.worker_uid,
                }
        )
        self._lua_restore_tasks = self.queue.register_script(
            self.REDIS_LUA_RESTORE_TASKS_FUNCTION_TEMPLATE \
                 % {
                    'received_tasks_queue_suffix': self.RECEIVED_TASKS_QUEUE_SUFFIX,
                    'tasks_restores_limit': self.RECEIVED_TASKS_RESTORE_COUNT_LIMIT,
                    'task_key_prefix': self.TASK_KEY_PREFIX,
                    'lock_suffix': self.LOCK_SUFFIX,
                }
        )

    def _generate_task_id(self):
        """Generate unique task id. We use one incrementing key in Redis for
        this.
        """
        return str(self.queue.incr(self.LAST_TASK_ID_KEY))

    def send(self, queue_name, item):
        """Enqueue element to specified queue in Redis.

        Returns unique task id.
        """
        task_id = None
        try:
            task_id = self._generate_task_id()
            self.queue.set(self.TASK_KEY_PREFIX + task_id, item)
            self.queue.lpush(self.QUEUE_KEY_PREFIX + queue_name, task_id)
        except redis.ConnectionError:
            if task_id is not None:
                logger.warning("Task was created, but connection failed in "
                    "the middle of the process. Task with id = %s probably "
                    "leaked.", task_id)
            raise BackendConnectionError
        return task_id

    def _pop_task(self, queue_name):
        """Smart implementation of poping task from queue to avoid tasks loss.
        """
        return self._lua_pop_task(args=[self.QUEUE_KEY_PREFIX + queue_name])

    def _restore_tasks(self, queue_name):
        """Helping method restoring (requeueing) possibly lost tasks.
        """
        return self._lua_restore_tasks(args=[self.QUEUE_KEY_PREFIX + queue_name])

    def receive(self, queue_name_list, timeout=0):
        """Safely dequeue element from Redis queue.
        Note: receive() should never lose a task!

        If timeout is 0, then block until receives a task.
        """
        if timeout == 0:
            timeout_interval = 1
        elif timeout >= 1:
            timeout_interval = timeout / 4.0
        else:
            timeout_interval = 0.2
        try:
            while 1:
                for queue_name in queue_name_list:
                    task = self._pop_task(queue_name)
                    if task:
                        return task
                # _lua_pop_task stores received tasks in another list in Redis,
                # so we have to return the tasks from that list to the original
                # queue once in a while. This is necessary in case when someone
                # received the task and died.
                # We will do this only if there is no tasks in requested queues.
                # Restore not more than one queue at a time.
                for queue_name in queue_name_list:
                    # Lock the restoring process for the queue
                    if self.queue.set(
                            self.QUEUE_KEY_PREFIX + queue_name + self.LOCK_SUFFIX,
                            self.worker_uid,
                            ex=self.RECEIVED_TASKS_RESTORE_INTERVAL, nx=True):
                        self._restore_tasks(queue_name)
                        break
                if timeout > 0:
                    timeout -= timeout_interval
                    if timeout <= 0:
                        return
                time.sleep(timeout_interval)
        except redis.ConnectionError as e:
            raise BackendConnectionError(e)

    # pylint: disable=W0613
    def keep_alive(self, task_id, queue_name=None):
        """Worker has to inform queue that it is still working on the task.
        As we use task locks, just update expiration time.
        In addition to updating expiration lock time, we check if task still
        exists.
        """
        task_exists = self.queue.exists(self.TASK_KEY_PREFIX + task_id)
        if not task_exists:
            return False
        # EXPIRE returns 1 if operation was successful and 0 otherwise.
        if self.queue.expire(self.TASK_KEY_PREFIX + task_id + self.LOCK_SUFFIX,
            self.TASK_LOCK_TIMEOUT) == 0:
            return False
        return True

    def delete(self, task_id, queue_name=None):
        """We delete the task information and Task ID will be removed from the
        queue's list eventually on the step of receiving new tasks.
        """
        self.queue.delete(self.TASK_KEY_PREFIX + task_id)

    # pylint: disable=W0613
    def acknowledge(self, task_id, queue_name=None):
        """Acknowledge the task (mark as done). In case of Redis backend it
        means that we have to delete the task information.
        """
        self.delete(task_id, queue_name=queue_name)

    def reject(self, task_id, queue_name=None):
        """Reject the task. In case of Redis backend it means that we have to
        delete the task information.
        """
        self.delete(task_id, queue_name=queue_name)
