#! /usr/bin/env python

import sys
import time
import logging
import unittest

from ConsistencyCheck import datatypes

class TestFlags(unittest.TestCase):
    new_files = [('/store/yo/0000/file_exists.root', 100, time.time())]
    inventory_files = [('/store/yo/0000/file_exists.root', 100),
                       ('/store/yo/0000/file_not_exists.root', 200)]

    def setUp(self):
        self.remote = datatypes.DirectoryInfo('/store')
        self.remote.add_file_list(self.new_files)
        self.invent = datatypes.DirectoryInfo('/store')
        self.invent.add_file_list(self.inventory_files)

        self.remote.setup_hash()
        self.invent.setup_hash()

    def test_directory_flag(self):
        self.assertFalse(self.remote.get_node('yo/0000').can_compare)

    def test_missing_file(self):
        self.assertEqual(self.invent.compare(self.remote), ([], [], 0))

if __name__ == '__main__':

    if len(sys.argv) > 1:
        logging.basicConfig(level=logging.DEBUG)

    unittest.main()
