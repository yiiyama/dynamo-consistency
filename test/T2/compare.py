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
        print 'Usage: ./compare.py site-name [watch/debug list inv]'
        exit(0)

    site = sys.argv[1]

    basename = os.path.join(config.config_dict()['CacheLocation'], '%s_%s.pkl')

    remotename = basename % (site, 'remote')
    inventoryname = basename % (site, 'inventory')

    if 'debug' in sys.argv:
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s:%(levelname)s:%(name)s: %(message)s')
    elif 'watch' in sys.argv:
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s:%(levelname)s:%(name)s: %(message)s')

    if 'list' in sys.argv:
        site_tree = getsitecontents.get_site_tree(site)
        site_tree.save(remotename)
    else:
        site_tree = datatypes.get_info(remotename)

    if 'inv' in sys.argv:
        inv_tree = getinventorycontents.get_site_inventory(site)
        inv_tree.save(inventoryname)
    else:
        inv_tree = datatypes.get_info(inventoryname)

    datatypes.compare(inv_tree, site_tree, '%s_compare' % site)
