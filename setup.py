#!/usr/bin/env python
import os
import re

from setuptools import setup, find_packages


version = re.compile(r'VERSION\s*=\s*\((.*?)\)')


def get_package_version():
    "returns package version without importing it"
    base = os.path.abspath(os.path.dirname(__file__))
    with open(os.path.join(base, "flower/__init__.py")) as initf:
        for line in initf:
            m = version.match(line.strip())
            if not m:
                continue
            return ".".join(m.groups()[0].split(", "))


def get_requirements(filename):
    return open('requirements/' + filename).read().splitlines()


classes = """
    Development Status :: 5 - Beta
    Intended Audience :: Developers
    License :: OSI Approved :: BSD License
    Topic :: System :: Distributed Computing
    Programming Language :: Python
    Programming Language :: Python :: 3
    Programming Language :: Python :: 3.3
    Programming Language :: Python :: 3.4
    Programming Language :: Python :: 3.5
    Programming Language :: Python :: 3.6
    Programming Language :: Python :: 3.7
    Programming Language :: Python :: Implementation :: CPython
    Programming Language :: Python :: Implementation :: PyPy
    Operating System :: OS Independent
"""
classifiers = [s.strip() for s in classes.split('\n') if s]


setup(
    name='django-flower',
    version=get_package_version(),
    description='Django Celery Flower',
    long_description=open('README.rst').read(),
    # original
    # author='Mher Movsisyan',
    # author_email='mher.movsisyan@gmail.com',
    author='alex',
    author_email='alex@fabricadigital.com.br',
    url='https://github.com/alexsilva/django-flower',
    license='BSD',
    classifiers=classifiers,
    packages=find_packages(exclude=['tests', 'tests.*']),
    install_requires=get_requirements('default.txt'),
    test_suite="tests",
    tests_require=get_requirements('test.txt'),
    include_package_data=True
)
