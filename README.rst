distributed-queue
=================

Distributed queue library for Python, with system to support different backend to implement distribution of messages

Supported backends
==================
    
* Dummy - backend for testing only, uses Python standard queues
* Redis - backend that uses Redis as distributed passing mechanizm

Requirements
============

- python 2.7
- python-redis

Installation
============

**Distributed queue** can be installed using pip: ::
    
    pip install git+git://github.com/ProstoKSI/distributed-queue

Contributing
============

You are welcome to contribute more backends with the same interface.
Development of `distributed-queue` happens at github: https://github.com/ProstoKSI/distributed-queue

License
=======

Copyright (C) 2009-2013 ProstoKSI
This program is licensed under the MIT License (see LICENSE)

