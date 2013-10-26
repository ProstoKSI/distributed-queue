"""Module with different serizlization mechanizms"""

import json

class BaseSerializer(object):
    """Base class for serialization and deserialization"""

    @staticmethod
    def dumps(obj):
        """Serialize given object."""
        raise NotImplementedError()

    @staticmethod
    def loads(data):
        """De-serialize data string back to object"""
        raise NotImplementedError()


class JsonSerializer(BaseSerializer):
    """Json serializer to convert objects to json strings and back"""
   
    @staticmethod
    def dumps(obj):
        """Serialize given object to JSON-string"""
        return json.dumps(obj)

    @staticmethod
    def loads(data):
        """De-serialize given JSON-string to object"""
        return json.loads(data)

