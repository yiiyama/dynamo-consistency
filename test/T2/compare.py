#! /usr/bin/env python

import logging
import sys

from ConsistencyCheck import getsitecontents
from ConsistencyCheck import getinventorycontents
from ConsistencyCheck import datatypes

if __name__ == '__main__':
    if 'debug' in sys.argv:
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s:%(levelname)s:%(name)s: %(message)s')

    if 'list' in sys.argv:
        site_tree = getsitecontents.get_site_tree('T2_US_MIT')
        site_tree.save('remote.pkl')
    else:
        site_tree = datatypes.get_info('remote.pkl')

    if 'inv' in sys.argv:
        inv_tree = getinventorycontents.get_site_inventory('T2_US_MIT')
        inv_tree.save('inventory.pkl')
    else:
        inv_tree = datatypes.get_info('inventory.pkl')

    datatypes.compare(inv_tree, site_tree, 'Compare')
