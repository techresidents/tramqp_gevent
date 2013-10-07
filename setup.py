import os
from setuptools import setup, find_packages

import ez_setup
ez_setup.use_setuptools()

setup(
    name='tramqp_gevent',
    version = '0.1-SNAPSHOT',
    author = 'Tech Residents, Inc.',
    packages = find_packages(),
    license = open('LICENSE').read(),
    description = 'Tech Residents AMQP GEvent Library',
    long_description = open('README').read(),
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: Other/Proprietary License',
        'Operating System :: POSIX',
        'Programming Language :: Python',
        'Topic :: Internet :: WWW/HTTP',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: System :: Distributed Computing',
        'Topic :: Utilities',
        ],
    install_requires=[
        'greenlet>=0.4.0',
        'gevent>=0.13.7',
        'trpycore>=0.13.0',
        'trpycore>=0.1.0'
    ],
    dependency_links=[
        'git+ssh://dev.techresidents.com/tr/repos/techresidents/lib/python/trpycore.git@0.13.0#egg=trpycore-0.13.0',
        'git+ssh://dev.techresidents.com/tr/repos/techresidents/lib/python/tramqp.git@0.1.0#egg=tramqp-0.1.0'
    ],
)
