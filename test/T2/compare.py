#! /usr/bin/env python

import logging
import sys

from ConsistencyCheck import getsitecontents
from ConsistencyCheck import getinventorycontents
from ConsistencyCheck import datatypes

if __name__ == '__main__':
    site = 'T2_US_MIT'

    if 'watch' in sys.argv:
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s:%(levelname)s:%(name)s: %(message)s')

    if 'list' in sys.argv:
        site_tree = getsitecontents.get_site_tree(site)
        site_tree.save('%s_remote.pkl' % site)
    else:
        site_tree = datatypes.get_info('%s_remote.pkl' % site)

    if 'inv' in sys.argv:
        inv_tree = getinventorycontents.get_site_inventory(site)
        inv_tree.save('%s_inventory.pkl' % site)
    else:
        inv_tree = datatypes.get_info('%s_inventory.pkl' % site)

    datatypes.compare(inv_tree, site_tree, '%s_compare' % site)
