import unittest

from distributed_queue.routers import BaseRouter, DefaultRouter

class TestRouters(unittest.TestCase):

    def test_base_router(self):
        router = BaseRouter()
        self.assertRaises(NotImplementedError, router.get_queue_name, {}, 'test', [], {})

    def test_default_router(self):
        default_queue_name = 'queue'
        router = DefaultRouter(default_queue_name)
        self.assertEquals(default_queue_name, router.get_queue_name({}, 'test', [], {}))

