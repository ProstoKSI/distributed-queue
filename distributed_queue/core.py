"""Core module for Distributed Queue.
Implenents functionality to work with backends.
"""

import os
import sys
import pkgutil
import inspect

from distributed_queue import backends

def load_all_modules_from_dir(dirname):
    """Import all python modules from dirname and return list of them"""
    module_list = []
    package_name = '.'.join(os.path.split(dirname))
    for importer, module_name, _ in pkgutil.iter_modules([dirname]):
        full_module_name = '%s.%s' % (package_name, module_name)
        if full_module_name in sys.modules:
            module = sys.modules[full_module_name]
        else:
            module = importer.find_module(module_name).load_module(module_name)
        module_list.append((module_name, module))
    return module_list

def load_all_backends(dirname, backend_base_class):
    """Load all python modules from `dirname` folder and enumerate all classes in them.
    Classes that are inherited from `backend_base_class` will be added as backends with name
    defined by module name.
    """
    module_list = load_all_modules_from_dir(dirname)
    backend_list = {}
    for module_name, module in module_list:
        for _, cls in inspect.getmembers(module):
            if inspect.isclass(cls) and cls != backend_base_class and \
                issubclass(cls, backend_base_class):
                backend_name = getattr(cls, 'BACKEND_NAME', module_name)
                backend_list[backend_name] = cls
    return backend_list

BACKEND_LIST = load_all_backends(os.path.dirname(backends.__file__), backends.BaseBackend)

def create_backend(name, *args, **kwargs):
    """Create backend by name and additional arguments.
    Name should be in BACKENDS_LIST, if it's not present there - None will be returned
    """
    if name not in BACKEND_LIST:
        return None
    return BACKEND_LIST[name](*args, **kwargs)

