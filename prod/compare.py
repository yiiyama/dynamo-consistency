#! /usr/bin/env python

import logging
import sys
import os
import sqlite3
import shutil
import time

from collections import defaultdict

from ConsistencyCheck import getsitecontents
from ConsistencyCheck import getinventorycontents
from ConsistencyCheck import datatypes
from ConsistencyCheck import config
from ConsistencyCheck import checkphedex

from CMSToolBox.webtools import get_json
from common.interface.mysql import MySQL

LOG = logging.getLogger(__name__)

def main(site):
    start = time.time()
    webdir = '/home/dabercro/public_html/ConsistencyCheck'

    site_tree = getsitecontents.get_site_tree(site)
    inv_tree = getinventorycontents.get_db_listing(site)

    # Create the function to check orphans
    acceptable_orphans = checkphedex.set_of_deletions(site)
    acceptable_missing = checkphedex.set_of_deletions(site)

    inv_sql = MySQL(config_file='/etc/my.cnf', db='dynamo', config_group='mysql-dynamo')
    inv_datasets = inv_sql.query(
        """
        SELECT datasets.name FROM sites
        INNER JOIN dataset_replicas ON dataset_replicas.site_id=sites.id
        INNER JOIN datasets ON dataset_replicas.dataset_id=datasets.id
        WHERE sites.name=%s
        """,
        site)

    acceptable_orphans.update(inv_datasets)
    acceptable_orphans.update(inv_sql.query('SELECT name FROM datasets WHERE status=%s', 'IGNORED'))

    protected_unmerged = get_json('cmst2.web.cern.ch', '/cmst2/unified/listProtectedLFN.txt')
    acceptable_orphans.update(['/%s/%s-%s/%s' % (split_name[4], split_name[3], split_name[6], split_name[5]) for split_name in \
                                   [name.split('/') for name in protected_unmerged['protected']]])

    LOG.debug('Acceptable orphans: \n%s\n', '\n'.join(acceptable_orphans))

    def double_check(file_name, acceptable=acceptable_orphans):
        LOG.debug('Checking file_name: %s', file_name)
        split_name = file_name.split('/')
        try:
            return '/%s/%s-%s/%s' % (split_name[4], split_name[3], split_name[6], split_name[5]) in acceptable
        except:
            LOG.warning('Strange file name: %s', file_name)
            return True

    check_missing = lambda x: double_check(x, acceptable_missing)

    # Do the comparison
    missing, m_size, orphan, o_size = datatypes.compare(inv_tree, site_tree, '%s_compare' % site,
                                                        orphan_check=double_check, missing_check=check_missing)

    # Reset things for site in register
    if site == 'T2_US_MIT':
        reg_sql = MySQL(config_file='/home/dabercro/my.cnf', db='dynamoregister', config_group='mysql-t3serv009')
    else:
        reg_sql = MySQL(config_file='/etc/my.cnf', db='dynamoregister', config_group='mysql-dynamo')

    no_source_files = []

    for line in missing:

        sites = inv_sql.query(
            """
            SELECT sites.name FROM sites
            INNER JOIN block_replicas ON sites.id = block_replicas.site_id
            INNER JOIN files ON block_replicas.block_id = files.block_id
            WHERE files.name = %s AND sites.name != %s
            AND sites.status != 'morgue' AND sites.status != 'unknown'
            AND block_replicas.is_complete = 1
            """,
            line, site)

        if sites:
            for location in sites:
                reg_sql.query(
                    """
                    INSERT IGNORE INTO `transfer_queue`
                    (`file`, `site_from`, `site_to`, `status`, `reqid`)
                    VALUES (%s, %s, %s, 'new', 0)
                    """,
                    line, location, site)

        else:
            no_source_files.append(line)


    with open('%s_missing_nosite.txt' % site, 'w') as nosite:
        for line in no_source_files:
            nosite.write(line + '\n')


    for line in orphan + site_tree.empty_nodes_list():
        reg_sql.query(
            """
            INSERT IGNORE INTO `deletion_queue`
            (`file`, `site`, `created`) VALUES
            (%s, %s, NOW())
            """,
            line, site)

    reg_sql.close()

    track_missing_blocks = defaultdict(
        lambda: { 'errors': 0, 
                  'blocks': defaultdict(lambda: { 'group': '',
                                                  'errors': 0 }
                                        )})

    with open('%s_compare_missing.txt' % site, 'r') as input_file:
        for line in input_file:
            split_name = line.split('/')
            dataset = '/%s/%s-%s/%s' % (split_name[4], split_name[3], split_name[6], split_name[5])

            output = inv_sql.query(
                """
                SELECT blocks.name, IFNULL(groups.name, 'Unsubscribed') FROM blocks
                INNER JOIN files ON files.block_id = blocks.id
                INNER JOIN block_replicas ON block_replicas.block_id = files.block_id
                INNER JOIN sites ON block_replicas.site_id = sites.id
                LEFT JOIN groups ON block_replicas.group_id = groups.id
                WHERE files.name = %s AND sites.name = %s
                """,
                line.strip(), site)

            if not output:
                print ("""
                SELECT blocks.name, IFNULL(groups.name, 'Unsubscribed') FROM blocks
                INNER JOIN files ON files.block_id = blocks.id
                INNER JOIN block_replicas ON block_replicas.block_id = files.block_id
                INNER JOIN sites ON block_replicas.site_id = sites.id
                LEFT JOIN groups ON block_replicas.group_id = groups.id
                WHERE files.name = %s AND sites.name = %s
                """ % (line.strip(), site))

                exit(1)

            block, group = output[0]

            track_missing_blocks[dataset]['errors'] += 1
            track_missing_blocks[dataset]['blocks'][block]['errors'] += 1
            track_missing_blocks[dataset]['blocks'][block]['group'] = group

    inv_sql.close()

    with open('%s_missing_datasets.txt' % site, 'w') as output_file:
        for dataset, vals in \
                sorted(track_missing_blocks.iteritems(),
                       key=lambda x: x[1]['errors'],
                       reverse=True):

            for block_name, block in sorted(vals['blocks'].iteritems()):
                output_file.write('%10i    %-17s  %s#%s\n' % \
                                      (block['errors'], block['group'],
                                       dataset, block_name))

    shutil.copy('%s_missing_datasets.txt' % site, webdir)
    shutil.copy('%s_missing_nosite.txt' % site, webdir)
    shutil.copy('%s_compare_missing.txt' % site, webdir)
    shutil.copy('%s_compare_orphan.txt' % site, webdir)

    # Update the runtime stats on the stats page
    conn = sqlite3.connect(os.path.join(webdir, 'stats.db'))
    curs = conn.cursor()

    curs.execute('REPLACE INTO stats_v4 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, DATETIME(DATETIME(), "-4 hours"), ?)',
                 (site, time.time() - start, site_tree.get_num_files(),
                  site_tree.count_nodes(), len(site_tree.empty_nodes_list()),
                  config.config_dict().get('NumThreads', config.config_dict().get('MinThreads', 0)),
                  len(missing), m_size, len(orphan), o_size, len(no_source_files)))

    conn.commit()
    conn.close()


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print 'Usage: ./compare.py sitename [sitename ...] [debug/watch]'
        exit(0)

    sites = sys.argv[1:-1]

    # Set the logging level based on the verbosity option

    logging_format = '%(asctime)s:%(levelname)s:%(name)s: %(message)s'

    if 'debug' in sys.argv:
        logging.basicConfig(level=logging.DEBUG, format=logging_format)
    elif 'watch' in sys.argv:
        logging.basicConfig(level=logging.INFO, format=logging_format)

    # If no valid verbosity level, assume the last arg was a sitename
    else:
        sites.append(sys.argv[-1])

    LOG.info('About to run over %s', sites)

    for site in sites:
        main(site)
