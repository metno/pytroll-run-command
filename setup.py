#!/usr/bin/python
# Copyright (c) 2017.
#

# Author(s):
#   Trygve Aspenes <trygveas@met.no>

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#

"""Not empty
"""

from setuptools import setup

setup(name="pytroll-run-command",
      version="0.1.0",
      description='Pytroll run a configured command in the pytroll posttroll environment',
      author='Trygve Aspenes',
      author_email='trygveas@met.no',
      classifiers=["Development Status :: 4 - Beta",
                   "Intended Audience :: Science/Research",
                   "License :: OSI Approved :: GNU General Public License v3 " +
                   "or later (GPLv3+)",
                   "Operating System :: OS Independent",
                   "Programming Language :: Python",
                   "Topic :: Scientific/Engineering"],
      url="git@gitlab.met.no:obs-fm-pytroll/pytroll-check-and-update-message.git",
      scripts=['bin/pytroll-run-command.py'],
      data_files=[],
      packages=[],
      zip_safe=False,
      install_requires=['posttroll', 'trollsift'])
