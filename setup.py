#!/usr/bin/env python

from distutils.core import setup

setup(name='iXsystems Installer',
      verion='0.5',
      description='iXSystems OS Installation',
      author='Sean Eric Fagan',
      author_email='sef@ixsystems.com',
      packages=['ixsystems'],
      data_files=[('/etc', ['etc/rc', 'etc/install.sh'])],
)
