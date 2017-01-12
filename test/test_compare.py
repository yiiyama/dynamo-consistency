#!/usr/bin/env python

"""
This is designed to test all the differences we would expect to see
between a file system and the inventory.

We want those differences to be accounted for correctly.
"""

import sys
import time
import logging
import unittest

from ConsistencyCheck import datatypes

if __name__ == '__main__':

    if len(sys.argv) > 1:
        logging.basicConfig(level=logging.DEBUG)

    unittest.main()
