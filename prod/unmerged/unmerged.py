#! /usr/bin/env python

# pylint: disable=wrong-import-position, too-complex, too-many-locals, too-many-branches, maybe-no-member, too-many-statements

"""
Needs to be written

:author: Daniel Abercrombie <dabercro@mit.edu>
"""

import logging
import json
import sys
import os
import sqlite3
import shutil
import time
import datetime

from collections import defaultdict

from dynamo_consistency import config
from common.interface.mysql import MySQL

# Stick this here before dynamo sets the logging config
if __name__ == '__main__':
    if len(sys.argv) < 2 or '-h' in sys.argv or '--help' in sys.argv:
        print 'Usage: ./compare.py sitename [sitename ...] [debug/watch]'
        exit(0)

    # Set the config file to read locally
    config.CONFIG_FILE = os.path.join(os.path.dirname(__file__),
                                      'consistency_config.json')

    SITES = sys.argv[1:-1]

    # Set the logging level based on the verbosity option

    LOG_FORMAT = '%(asctime)s:%(levelname)s:%(name)s: %(message)s'

    if 'debug' in sys.argv:
        logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)
    elif 'watch' in sys.argv:
        logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

    # If no valid verbosity level, assume the last arg was a sitename
    else:
        SITES.append(sys.argv[-1])


from dynamo_consistency import getsitecontents

import ListDeletable

LOG = logging.getLogger(__name__)

def main(site):
    """
    Gets the listing from the dynamo database, and remote XRootD listings of a given site.
    The differences are compared to deletion queues and other things.

    .. Note::
       If you add things, list them in the module docstring.

    The differences that should be acted on are copied to the summary webpage
    and entered into the dynamoregister database.

    :param str site: The site to run the check over
    :returns: missing files, size, orphan files, size
    :rtype: list, long, list, long
    """

    start = time.time()
    is_dst = time.localtime().tm_isdst
    if is_dst == -1:
        LOG.error('Daylight savings time not known. Times on webpage will be rather wrong.')

    prev_missing = '%s_compare_missing.txt' % site
    prev_set = set()

    config_dict = config.config_dict()

    if os.path.exists(prev_missing):
        with open(prev_missing, 'r') as prev_file:
            for line in prev_file:
                prev_set.add(line.strip())

        if int(config_dict.get('SaveCache')):
            prev_new_name = '%s.%s' % (prev_missing,
                                       datetime.datetime.fromtimestamp(
                                           os.stat(prev_missing).st_mtime).strftime('%y%m%d')
                                      )
        else:
            prev_new_name = prev_missing

        shutil.move(prev_missing,
                    os.path.join(config_dict['CacheLocation'],
                                 prev_new_name)
                   )

    # All of the files and summary will be dumped here
    webdir = config_dict['WebDir']

    # Open a connection temporarily to make sure we only list good sites
    status_check = MySQL(config_file='/etc/my.cnf', db='dynamo', config_group='mysql-dynamo')
    status = status_check.query('SELECT status FROM sites WHERE name = %s', site)[0]

    if status != 'ready':
        LOG.error('Site %s status is %s', site, status)

        # Note the attempt to do listing
        conn = sqlite3.connect(os.path.join(webdir, 'stats.db'))
        curs = conn.cursor()
        curs.execute(
            """
            REPLACE INTO stats VALUES
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, DATETIME(DATETIME(), "-{0} hours"), ?, ?)
            """.format(5 - is_dst),
            (site, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))

        conn.commit()
        conn.close()

        exit(0)

    # Close the connection while we are getting the trees together
    status_check.close()

    site_tree = getsitecontents.get_site_tree(site)

    # Get whether or not the site is debugged
    conn = sqlite3.connect(os.path.join(webdir, 'stats.db'))
    curs = conn.cursor()
    curs.execute('SELECT isgood FROM sites WHERE site = ?', (site, ))
    is_debugged = curs.fetchone()[0]
    conn.close()

    # Do the comparison
    deletion_file = site + ListDeletable.config.DELETION_FILE
    ListDeletable.config.DELETION_FILE = deletion_file

    ListDeletable.PROTECTED_LIST = ListDeletable.get_protected()
    ListDeletable.PROTECTED_LIST.sort()

    ListDeletable.get_unmerged_files = lambda: site_tree.get_files(ListDeletable.config.MIN_AGE) # Only consider things older than four weeks
    ListDeletable.main()

    # Enter things for site in registry
    if os.environ['USER'] == 'dynamo':
        reg_sql = MySQL(config_file='/etc/my.cnf',
                        db='dynamoregister', config_group='mysql-dynamo')
    else:
        reg_sql = MySQL(config_file=os.path.join(os.environ['HOME'], 'my.cnf'),
                        db='dynamoregister', config_group='mysql-register-test')

    # Determine if files should be entered into the registry

    if is_debugged:
        def execute(query, *args):
            """
            Executes the query on the registry and outputs a log message depending on query

            :param str query: The SQL query to execute
            :param args: The arguments to the SQL query
            """

            reg_sql.query(query, *args)
    else:
        execute = lambda *_: 0


    # Only get the empty nodes that are not in the inventory tree
    for line in open(deletion_file, 'r'):
        execute(
            """
            INSERT IGNORE INTO `deletion_queue`
            (`file`, `site`, `status`) VALUES
            (%s, %s, 'new')
            """,
            line.strip(), site)


    reg_sql.close()


if __name__ == '__main__':

    LOG.info('About to run over %s', SITES)

    for site_to_check in SITES:
        main(site_to_check)
