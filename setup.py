import os
from setuptools import setup, find_packages

from distributed_queue import VERSION, PROJECT

META_DATA = dict(
    name=PROJECT,
    version=VERSION,

    author="ProstoKSI",
    author_email="prostoksi@gmail.com",
    url="http://prostoksi.com/",

    packages=find_packages(),

    install_requires = [
        'redis',
    ]
)

if __name__ == "__main__":
    setup(**META_DATA)

