#! /usr/bin/env python

from ConsistencyCheck import getsitecontents
from ConsistencyCheck import getinventorycontents
from ConsistencyCheck import datatypes

if __name__ == '__main__':
    site_tree = getsitecontents.get_site_tree('T2_US_MIT')
    inv_tree = getinventorycontents.get_site_inventory('T2_US_MIT')

    site_tree.save('remote.pkl')
    inv_tree.save('inventory.pkl')

    datatypes.compare(inv_tree, site_tree, 'Compare')
