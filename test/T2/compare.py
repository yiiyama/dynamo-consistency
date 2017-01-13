#! /usr/bin/env python

import logging
import sys

from ConsistencyCheck import getsitecontents
from ConsistencyCheck import getinventorycontents
from ConsistencyCheck import datatypes

if __name__ == '__main__':
    if 'debug' in sys.argv:
        logging.basicConfig(level=logging.DEBUG)

    if 'list' in sys.argv:
        site_tree = getsitecontents.get_site_tree('T2_US_MIT')
        inv_tree = getinventorycontents.get_site_inventory('T2_US_MIT')

        site_tree.save('remote_results.pkl')
        inv_tree.save('inventory_results.pkl')

    else:
        site_tree = datatypes.get_info('remote_results.pkl')
        inv_tree = datatypes.get_info('inventory_results.pkl')

    datatypes.compare(inv_tree, site_tree, 'Compare')
