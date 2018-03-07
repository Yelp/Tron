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
        'Programming Language :: Python :: 2.7',
        "Operating System :: OS Independent",
        "License :: OSI Approved :: Apache Software License",
        "Topic :: System :: Monitoring",
        "Topic :: System :: Systems Administration",
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators",
        "Development Status :: 4 - Beta",
    ],
    install_requires=[
        'humanize >= 0.5.0',
        'Twisted >=10.0.0, <=12.3',
        'PyYAML>=3.0',
        'pyasn1>=0.0.13',
        'pycrypto>=2.4',
        'pytz>=2011n',
        'pysensu-yelp',
        'python-daemon',
        'lockfile>=0.7',
        'six>=1.11.0',
        'SQLAlchemy>=1.0.15',
        'yelp-clog',
        'enum34>=1.1.6',
    ],
    packages=find_packages(exclude=['tests.*', 'tests']) + ['tronweb'],
    scripts=glob.glob('bin/*'),
    include_package_data=True,
    long_description="""Tron is a centralized system for managing periodic batch processes and services across a cluster. If you find cron or fcron to be insufficient for managing complex work flows across multiple computers, Tron might be for you.

For more information, look at the tutorial (http://packages.python.org/tron/tutorial.html) or the full documentation (http://packages.python.org/tron).""",
)
