#! /usr/bin/env python

# pylint: disable=wrong-import-position, too-complex, too-many-locals, too-many-branches, maybe-no-member, too-many-statements, ungrouped-imports

"""
Needs to be written

:author: Daniel Abercrombie <dabercro@mit.edu>
"""

import logging
import sys
import os

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
    """

    # Open a connection temporarily to make sure we only list good sites
    status_check = MySQL(config_file='/etc/my.cnf', db='dynamo', config_group='mysql-dynamo')
    status = status_check.query('SELECT status FROM sites WHERE name = %s', site)[0]

    if status != 'ready':
        LOG.error('Site %s status is %s', site, status)
        exit(0)

    # Close the connection while we are getting the trees together
    status_check.close()

    site_tree = getsitecontents.get_site_tree(site)

    # Do the unmerged cleaning
    deletion_file = site + ListDeletable.config.DELETION_FILE
    ListDeletable.config.DELETION_FILE = deletion_file

    ListDeletable.PROTECTED_LIST = ListDeletable.get_protected()
    ListDeletable.PROTECTED_LIST.sort()

    # Only consider things older than four weeks
    ListDeletable.get_unmerged_files = lambda: site_tree.get_files(ListDeletable.config.MIN_AGE)
    ListDeletable.main()

    # Enter things for site in registry
    if os.environ['USER'] == 'dynamo' or site == 'T2_US_MIT':
        reg_sql = MySQL(config_file='/etc/my.cnf',
                        db='dynamoregister', config_group='mysql-dynamo')
    else:
        reg_sql = MySQL(config_file=os.path.join(os.environ['HOME'], 'my.cnf'),
                        db='dynamoregister', config_group='mysql-register-test')

    # Determine if files should be entered into the registry

    def execute(query, *args):
        """
        Executes the query on the registry and outputs a log message depending on query

        :param str query: The SQL query to execute
        :param args: The arguments to the SQL query
        """

        reg_sql.query(query, *args)

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
