#!/usr/bin/env python

from distutils.core import setup

from setuptools import find_packages


setup(name='ikats',
      version='0.0',
      description='IKATS Python API',
      author='Fabien TORTORA',
      author_email='fabien.tortora@c-s.fr',
      url='https://www.ikats.org',
      packages=find_packages(),
      setup_requires=['nose>=1.3.7', 'coverage'],
      install_requires=["numpy>=1.15.4", 'requests>=2.21.0', 'schema>=0.6.8'],
      keywords='timeseries, big data, spark',
      license='Apache License 2.0',
      test_suite='nose.collector',
      )
