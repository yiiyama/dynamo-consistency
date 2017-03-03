#! /usr/bin/env python

import logging
import sys
import os

from ConsistencyCheck import getsitecontents
from ConsistencyCheck import getinventorycontents
from ConsistencyCheck import datatypes
from ConsistencyCheck import config

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print 'Usage: ./compare.py site-name [debug/watch]'
        exit(0)

    site = sys.argv[1]

    if 'debug' in sys.argv:
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s:%(levelname)s:%(name)s: %(message)s')
    elif 'watch' in sys.argv:
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s:%(levelname)s:%(name)s: %(message)s')

    site_tree = getsitecontents.get_site_tree(site)
    inv_tree = getinventorycontents.get_site_inventory(site)

    datatypes.compare(inv_tree, site_tree, '%s_compare' % site)
