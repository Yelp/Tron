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
    install_requires=[
        'argcomplete==1.9.4',
        'bsddb3==6.2.6',
        'cryptography==2.3.1',
        'dataclasses==0.6',
        'enum34==1.1.6',
        'humanize==0.5.1',
        'ipdb==0.11',
        'ipython==6.5.0',
        'lockfile==0.12.2',
        'psutil==5.4.7',
        'pyasn1==0.4.4',
        'pyformance==0.4',
        'pysensu-yelp==0.4.0',
        'pytimeparse==1.1.8',
        'python-daemon==2.2.0',
        'pytz==2018.5',
        'PyYAML==3.13',
        'requests==2.19.1',
        'six==1.11.0',
        'SQLAlchemy==1.2.12',
        'task_processing[mesos_executor]==0.1.2',
        'Twisted==18.7.0',
        'yelp-clog==2.16.0',
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
