from distutils.core import setup

setup(
    name="tron",
    version='0.1.6',
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
        "Development Status :: 3 - Alpha",
    ],
    requires = ['twisted (>=10.0.0)',],
    packages=["tron", "tron.utils"],
    scripts=['bin/trond', 'bin/tronview', 'bin/tronctl', 'bin/tronfig'],
    data_files=[('share/doc/tron', ['docs/sample_config_large.yaml', 'docs/sample_config_small.yaml']),],
    long_description="""\
Tron is a job scheduling, running and monitoring package designed to replace Cron for complex job running requirements.
  - Centralized configuration for running jobs across multiple machines
  - Dependencies on jobs and resources
  - Monitoring of jobs
	"""
)
