try:
    from setuptools import setup, find_packages
    assert setup
except ImportError:
    from distutils.core import setup

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
        'Programming Language :: Python :: 2.5',
        'Programming Language :: Python :: 2.6',
        "Operating System :: OS Independent",
        "License :: OSI Approved :: Apache Software License",
        "Topic :: System :: Monitoring",
        "Topic :: System :: Systems Administration",
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators",
        "Development Status :: 4 - Beta",
    ],
    install_requires=[
        'Twisted >=10.0.0, <=12.3',
        'PyYAML>=3.0',
        'pyasn1>=0.0.13',
        'pycrypto>=2.4',
        'pytz>=2011n',
        'python-daemon>=1.5.2',
        'lockfile>=0.7',
    ],
    packages=find_packages(exclude=['tests.*','tests'])+['tronweb'],
    scripts=[
        'bin/trond',
        'bin/tronview',
        'bin/tronctl',
        'bin/tronfig',
        'bin/action_runner.py',
        'bin/action_status.py',
    ],
    include_package_data=True,
    long_description="""Tron is a centralized system for managing periodic batch processes and services across a cluster. If you find cron or fcron to be insufficient for managing complex work flows across multiple computers, Tron might be for you.

For more information, look at the tutorial (http://packages.python.org/tron/tutorial.html) or the full documentation (http://packages.python.org/tron).""",
)
