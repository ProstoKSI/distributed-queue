import unittest

from distributed_queue.serializers import BaseSerializer, JsonSerializer

class TestSerializers(unittest.TestCase):
    
    def test_base_serializer(self):
        self.assertRaises(NotImplementedError, BaseSerializer.dumps, {})
        self.assertRaises(NotImplementedError, BaseSerializer.loads, "{}")

    def test_json_serializer(self):
        serializer = JsonSerializer
        obj = {"a": 1, "b": [1, 2, "3"]}
        data = serializer.dumps(obj)
        copy_obj = serializer.loads(data)
        self.assertEqual(obj, copy_obj)

