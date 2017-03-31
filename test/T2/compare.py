#! /usr/bin/env python

import logging
import sys
import os
import sqlite3
import shutil
import time

from ConsistencyCheck import getsitecontents
from ConsistencyCheck import getinventorycontents
from ConsistencyCheck import datatypes
from ConsistencyCheck import config


def main(site):
    start = time.time()

    site_tree = getsitecontents.get_site_tree(site)

    webdir = '/home/dabercro/public_html/ConsistencyCheck'

    try:
        inv_tree = getinventorycontents.get_site_inventory(site)

        missing, orphan = datatypes.compare(inv_tree, site_tree, '%s_compare' % site)


        shutil.copy('%s_compare_missing.txt', webdir)
        shutil.copy('%s_compare_orphan.txt', webdir)
    except:
        missing = []
        orphan = []


    conn = sqlite3.connect(os.path.join(webdir, 'stats.db'))
    curs = conn.cursor()

    curs.execute('REPLACE INTO stats VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                 (site, time.time() - start, site_tree.get_num_files(),
                  site_tree.count_nodes(), site_tree.count_nodes(True),
                  config.config_dict().get('NumThreads', config.config_dict().get('MinThreads', 0)),
                  len(missing), len(orphan)))

    conn.commit()
    conn.close()


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

    main(site)
