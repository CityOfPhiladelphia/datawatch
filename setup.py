#!/usr/bin/env python

from distutils.core import setup

setup(
    name='DataWatch',
    version='0.1.0',
    packages=['datawatch',],
    install_requires=[
        'click==6.7',
        'cx-Oracle==6.0.2',
        'psycopg2==2.7.3.1',
        'PyYAML==3.12',
        'requests==2.18.4',
        'sqlalchemy>=1.0,<2.0a',
    ],
    entry_points={
        'console_scripts': [
            'datawatch=datawatch:main',
        ],
    },
)
