#! /usr/bin/env python

# Simply lists a single site and times it

import logging
import time

from dynamo_consistency import getsitecontents

# Set this however you'd like
logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(levelname)s:%(name)s: %(message)s')

if __name__ == '__main__':
    start = time.time()
    tree = getsitecontents.get_site_tree('T2_US_Nebraska')
    print '\nDuration: %f seconds\n' % (time.time() - start)
