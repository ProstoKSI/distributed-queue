import re
from setuptools import setup, find_packages

with open('distributed_queue/version.py') as version_file:
    __version__ = re.findall("__version__ = '([^']*)'", version_file.read())[0]

META_DATA = dict(
    name='distributed-queue',
    version=__version__,

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

