from __future__ import absolute_import
from __future__ import unicode_literals
try:
    from setuptools import setup, find_packages
    assert setup
except ImportError:
    from distutils.core import setup

import glob
import tron

setup(
    name="tron",
    version=tron.__version__,
    provides=['tron'],
    author="Yelp",
    author_email="yelplabs@yelp.com",
    url="http://github.com/Yelp/Tron",
    description='Job scheduling and monitoring system',
    classifiers=[
        "Programming Language :: Python",
        'Programming Language :: Python :: 3.6',
        "Operating System :: OS Independent",
        "License :: OSI Approved :: Apache Software License",
        "Topic :: System :: Monitoring",
        "Topic :: System :: Systems Administration",
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators",
        "Development Status :: 4 - Beta",
    ],
    packages=find_packages(
        exclude=['tests.*', 'tests', 'example-cluster']
    ) + ['tronweb'],
    scripts=glob.glob('bin/*') + glob.glob('tron/bin/*.py'),
    include_package_data=True,
    long_description="""
Tron is a centralized system for managing periodic batch processes across a
cluster. If you find cron or fcron to be insufficient for managing complex work
flows across multiple computers, Tron might be for you.

For more information, look at the
`tutorial <http://tron.readthedocs.io/en/latest/tutorial.html>`_ or the
`full documentation <http://tron.readthedocs.io/en/latest/index.html>`_.
""",
)
