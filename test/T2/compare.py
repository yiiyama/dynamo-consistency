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

from common.interface.mysql import MySQL

def main(site):
    start = time.time()
    webdir = '/home/dabercro/public_html/ConsistencyCheck'

    site_tree = getsitecontents.get_site_tree(site)
    inv_tree = getinventorycontents.get_site_inventory(site)
    missing, orphan = datatypes.compare(inv_tree, site_tree, '%s_compare' % site)

    sql = MySQL(config_file='/etc/my.cnf', db='dynamoregister', config_group='mysql-dynamo')
    for line in missing:
        pass

    for line in orphan:
        sql.query('INSERT IGNORE INTO `deletion_queue` (`file`, `target`, `created`) VALUES (%s, %s, NOW())', line, site)

    shutil.copy('%s_compare_missing.txt' % site, webdir)
    shutil.copy('%s_compare_orphan.txt' % site, webdir)

    # Runtime stats
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
