#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (c) 2020 Trygve Aspenes
#
# satpy is free software: you can redistribute it and/or modify it under the
# terms of the GNU General Public License as published by the Free Software
# Foundation, either version 3 of the License, or (at your option) any later
# version.
#
# satpy is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
# A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along with
# satpy.  If not, see <http://www.gnu.org/licenses/>.
"""Test of test
"""
import unittest


class Testruncommand(unittest.TestCase):
    """Test the run command."""

    def setUp(self):
        """Create temporary directory to save files to."""
        import tempfile
        self.base_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Remove the temporary directory created for a test."""
        try:
            import shutil
            shutil.rmtree(self.base_dir, ignore_errors=True)
        except OSError:
            pass

    def test_init(self):
        """Test start."""
        from bin.pytroll_run_command import read_from_queue
