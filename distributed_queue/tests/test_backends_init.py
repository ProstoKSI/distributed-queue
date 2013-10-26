import unittest

from distributed_queue import core, backends

class TestBackend(unittest.TestCase):

    def test_list_backends(self):
        backend_list = core.BACKEND_LIST
        self.assertTrue('dummy' in backend_list)
        self.assertTrue('redis' in backend_list)

    def test_create_backend_fail(self):
        backend = core.create_backend('error')
        self.assertEqual(backend, None)

    def test_create_backend_base(self):
        backend = backends.BaseBackend()
        self.assertRaises(NotImplementedError, backend.send, 'test', 'test')
        self.assertRaises(NotImplementedError, backend.receive, ['test'])

    def test_create_backend_dummy(self):
        backend = core.create_backend('dummy')
        self.assertTrue(backend is not None)
        self.assertTrue(isinstance(backend, backends.BaseBackend))
        self.assertTrue(getattr(backend, 'send', None) is not None)
        self.assertTrue(getattr(backend, 'receive', None) is not None)
        test_data = 'test 1 2 3'
        backend.send('test', test_data)
        item = backend.receive(['test'])
        self.assertTrue(item == test_data)
        item = backend.receive(['other'])
        self.assertEqual(item, None)

    def test_create_backend_redis(self):
        #TODO: Need to test redis backend
        pass

