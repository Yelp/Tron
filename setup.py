try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

import tron

setup(
    name="tron",
    version=tron.__version__,
    provides="tron",
    author="Yelp",
    author_email="yelplabs@yelp.com",
    url="http://github.com/Yelp/Tron",
    description='Job scheduling and monitoring system',
    classifiers=[
        "Programming Language :: Python",
        "Operating System :: OS Independent",
        "License :: OSI Approved :: Apache Software License",
        "Topic :: System :: Monitoring",
        "Topic :: System :: Systems Administration",
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators",
        "Development Status :: 4 - Beta",
    ],
    requires = [
        'twisted (>=10.0.0)',
        'PyYAML (>=3.10)'
    ],
    packages=["tron", "tron.utils"],
    scripts=[
        'bin/trond',
        'bin/tronview',
        'bin/tronctl',
        'bin/tronfig'
    ],
    data_files=[
        ('share/doc/tron', [
        'docs/sample_config_large.yaml',
        'docs/sample_config_small.yaml']),
    ],
    long_description=open('README.md').read(),
)
