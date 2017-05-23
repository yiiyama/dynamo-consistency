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
    inv_tree = checkphedex.get_phedex_tree(site)
#    inv_tree = getinventorycontents.get_site_inventory(site)

    # Create the function to check orphans
    acceptable_orphans = checkphedex.set_of_deletions(site)

    inv_sql = MySQL(config_file='/etc/my.cnf', db='dynamo', config_group='mysql-dynamo')
    inv_datasets = inv_sql.query('SELECT datasets.name '
                                 'FROM sites INNER JOIN dataset_replicas INNER JOIN datasets '
                                 'WHERE dataset_replicas.dataset_id=datasets.id AND '
                                 'dataset_replicas.site_id=sites.id and sites.name=%s', site)

    acceptable_orphans.update(inv_datasets)

    def double_check(file_name):
        split_name = file_name.split('/')
        try:
            return '/%s/%s-%s/%s' % (split_name[4], split_name[3], split_name[6], split_name[5]) in acceptable_orphans
        except:
            print 'Strange file name: %s' % file_name
            return True

    # Do the comparison
    missing, m_size, orphan, o_size = datatypes.compare(inv_tree, site_tree, '%s_compare' % site, orphan_check=double_check)

    # Reset things for site in register
    sql = MySQL(config_file='/etc/my.cnf', db='dynamoregister', config_group='mysql-dynamo')
    sql.query('DELETE FROM `deletion_queue` WHERE `target`=%s', site)
    sql.query('DELETE FROM `transfer_queue` WHERE `target`=%s', site)

    for line in missing:
        # First, get potential locations of the file
        response = get_json(
            'cmsweb.cern.ch', '/phedex/datasvc/json/prod/filereplicas', {'lfn': line}, use_https=True)
        sites = [replica['node'] for replica in response['phedex']['block'][0]['file'][0]['replica'] \
                     if replica['node'] != site]

        # Get actual locations of the file
        hosts = config.locate_file(line)
        physical_sites = [get_site(host) for host in hosts]

        for source in sites:
            if source in physical_sites:

                sql.query(
                    'INSERT IGNORE INTO `transfer_queue` (`file`, `source`, `target`, `created`) VALUES (%s, %s, %s, NOW())',
                    line, source, site)
                break

    for line in orphan + site_tree.empty_nodes_list():
        sql.query('INSERT IGNORE INTO `deletion_queue` (`file`, `target`, `created`) VALUES (%s, %s, NOW())', line, site)

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

    print 'About to run over %s' % sites

    for site in sites:
        main(site)
