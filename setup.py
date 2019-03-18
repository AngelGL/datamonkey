#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import setup, find_packages

with open('README.md') as readme_file:
    readme = readme_file.read()

requirements = ['pandas', 'numpy', 'requests', 'ujson']

setup_requirements = ['pytest-runner', ]

test_requirements = ['pytest', ]

setup(
    author="Data Monkey, LLC",
    author_email='help@data-monkey.com',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    description="Data processing and transformation library.",
    install_requires=requirements,
    license="MIT license",
    long_description=readme,
    include_package_data=True,
    keywords='datamonkey',
    name='datamonkey',
    packages=find_packages(include=['datamonkey']),
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/datamonkeyhq/datamonkey',
    version='1.0.0',
    zip_safe=False,
)
