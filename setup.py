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
      install_requires=["numpy"],
      keywords='timeseries, classification, machine learning, big data, spark',
      license='Apache License 2.0'
      )
