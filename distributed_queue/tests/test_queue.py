import json
import unittest
from mock import Mock

from distributed_queue.queue import DistributedQueue, DistributedQueueError
from distributed_queue.routers import DefaultRouter
from distributed_queue.serializers import JsonSerializer

class TestQueue(unittest.TestCase):

    def test_push_queue_mock(self):
        backend = Mock()
        queue = DistributedQueue({'default': {'backend': backend, 'default_queue': 'test', 'queues': ['test']}})
        original_task = 'task'
        original_args = [1, 2, 3]
        original_kwargs = {'a': 1, 'b': 2}
        queue.send(original_task, original_args, original_kwargs)

    def test_pop_queue_mock(self):
        original_task = 'task'
        original_args = [1, 2, 3]
        original_kwargs = {'a': 1, 'b': 2}
        backend = Mock()
        backend.receive = Mock(return_value=json.dumps((original_task, original_args, original_kwargs)))
        queue = DistributedQueue({'default': {'backend': backend, 'default_queue': 'test', \
            'serializer': JsonSerializer(), 'queues': ['test']}})
        task, args, kwargs = queue.receive()
        self.assertTrue(task == original_task)
        self.assertTrue(args == original_args)
        self.assertTrue(kwargs == original_kwargs)
        backend.receive = Mock(return_value=None)
        item = queue.receive()
        self.assertEqual(item, None)

    def test_queue_dummy(self):
        queue = DistributedQueue({'default': {'backend': 'dummy', 'default_queue': 'test', 'queues': ['test']}})
        original_task = 'task'
        original_args = [1, 2, 3]
        original_kwargs = {'a': 1, 'b': 2}
        queue.send(original_task, *original_args, **original_kwargs)
        queue.send_custom(original_task, original_args, original_kwargs, queue_name='test')
        task, args, kwargs = queue.receive()
        self.assertTrue(task == original_task)
        self.assertTrue(args == original_args)
        self.assertTrue(kwargs == original_kwargs)
        task, args, kwargs = queue.receive(queue_name='test')
        self.assertTrue(task == original_task)
        self.assertTrue(args == original_args)
        self.assertTrue(kwargs == original_kwargs)

    def test_queue_dummy_specify_serializer_router(self):
        router = DefaultRouter('test')
        serializer = JsonSerializer()
        queue = DistributedQueue({'default': {'backend': 'dummy', 
            'router': router, 'serializer': serializer, 'queues': ['test']}})
        self.assertEqual(queue.backends['default']['router'], router)
        self.assertEqual(queue.backends['default']['serializer'], serializer)

    def test_queue_incorrect_settings(self):
        self.assertRaises(DistributedQueueError, DistributedQueue, {'default': {}})
        self.assertRaises(DistributedQueueError, DistributedQueue, {'default': {'backend': 'dummy'}})
        self.assertRaises(DistributedQueueError, DistributedQueue, {'default': {'backend': 'dummy', \
            'default_queue': 'test'}})
        self.assertRaises(DistributedQueueError, DistributedQueue, {'default': {'backend': 'dummy', \
            'queues': ['test']}})
        self.assertRaises(DistributedQueueError, DistributedQueue, {'default': {'backend': 'dummy', \
            'queues': ['test'], 'default_queue': 'other_test'}})

    def test_queue_incorrect_backend(self):
        self.assertRaises(DistributedQueueError, DistributedQueue, {'default': {'backend': object(), \
            'default_queue': 'test', 'queues': ['test']}})
        self.assertRaises(DistributedQueueError, DistributedQueue, {'default': {'backend': 'error', \
            'default_queue': 'test', 'queues': ['test']}})

