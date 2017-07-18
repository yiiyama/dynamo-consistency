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
from ConsistencyCheck import checkphedex

from CMSToolBox.webtools import get_json
from CMSToolBox.siteinfo import get_site
from common.interface.mysql import MySQL

def main(site):
    start = time.time()
    webdir = '/home/dabercro/public_html/ConsistencyCheck'

    site_tree = getsitecontents.get_site_tree(site)
    inv_tree = getinventorycontents.get_db_listing(site)
#    inv_tree = checkphedex.get_phedex_tree(site)

    # Create the function to check orphans
    acceptable_orphans = checkphedex.set_of_deletions(site)

    inv_sql = MySQL(config_file='/etc/my.cnf', db='dynamo', config_group='mysql-dynamo')
    inv_datasets = inv_sql.query('SELECT datasets.name FROM sites '
                                 'INNER JOIN dataset_replicas ON dataset_replicas.site_id=sites.id '
                                 'INNER JOIN datasets ON dataset_replicas.dataset_id=datasets.id '
                                 'WHERE sites.name=%s', site)

    acceptable_orphans.update(inv_datasets)
    acceptable_orphans.update(inv_sql.query('SELECT name FROM datasets WHERE status=%s', 'IGNORED'))

    protected_unmerged = get_json('cmst2.web.cern.ch', '/cmst2/unified/listProtectedLFN.txt')
    acceptable_orphans.update(['/%s/%s-%s/%s' % (split_name[4], split_name[3], split_name[6], split_name[5]) for split_name in \
                                   [name.split('/') for name in protected_unmerged['protected']]])

    def double_check(file_name):
        split_name = file_name.split('/')
        try:
            return '/%s/%s-%s/%s' % (split_name[4], split_name[3], split_name[6], split_name[5]) in acceptable_orphans
        except:
            logging.getLogger(__name__).warning('Strange file name: %s', file_name)
            return True

    # Do the comparison
    missing, m_size, orphan, o_size = datatypes.compare(inv_tree, site_tree, '%s_compare' % site, orphan_check=double_check)

    # Reset things for site in register
    if site == 'T2_US_MIT':
        reg_sql = MySQL(config_file='/home/dabercro/my.cnf', db='dynamoregister', config_group='mysql-t3serv009')
    else:
        reg_sql = MySQL(config_file='/etc/my.cnf', db='dynamoregister', config_group='mysql-dynamo')

#    reg_sql.query('DELETE FROM `deletion_queue` WHERE `site`=%s', site)
#    reg_sql.query('DELETE FROM `transfer_queue` WHERE `site_to`=%s', site)

    for line in missing:

        sites = inv_sql.query(
            'SELECT sites.name FROM sites '
            'INNER JOIN block_replicas ON sites.id = block_replicas.site_id '
            'INNER JOIN files ON block_replicas.block_id = files.block_id '
            'WHERE files.name = %s AND sites.name != %s',
            line, site)

        if sites:
            for location in sites:
                reg_sql.query(
                    'INSERT IGNORE INTO `transfer_queue` (`file`, `site_from`, `site_to`, `status`, `reqid`) VALUES (%s, %s, %s, \'new\', 0)',
                    line, location, site)

    for line in orphan + site_tree.empty_nodes_list():
        reg_sql.query('INSERT IGNORE INTO `deletion_queue` (`file`, `site`, `created`) VALUES (%s, %s, NOW())', line, site)

    inv_sql.close()
    reg_sql.close()
    shutil.copy('%s_compare_missing.txt' % site, webdir)
    shutil.copy('%s_compare_orphan.txt' % site, webdir)

    # Runtime stats
    conn = sqlite3.connect(os.path.join(webdir, 'stats.db'))
    curs = conn.cursor()

    curs.execute('REPLACE INTO stats_v3 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, DATETIME())',
                 (site, time.time() - start, site_tree.get_num_files(),
                  site_tree.count_nodes(), len(site_tree.empty_nodes_list()),
                  config.config_dict().get('NumThreads', config.config_dict().get('MinThreads', 0)),
                  len(missing), m_size, len(orphan), o_size))

    conn.commit()
    conn.close()


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print 'Usage: ./compare.py sitename [sitename ...] [debug/watch]'
        exit(0)

    sites = sys.argv[1:-1]

    if 'debug' in sys.argv:
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s:%(levelname)s:%(name)s: %(message)s')
    elif 'watch' in sys.argv:
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s:%(levelname)s:%(name)s: %(message)s')
    else:
        sites.append(sys.argv[-1])

    logging.getLogger(__name__).info('About to run over %s', sites)

    for site in sites:
        main(site)
