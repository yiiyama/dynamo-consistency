#! /usr/bin/env python

# pylint: disable=wrong-import-position, too-complex, too-many-locals, too-many-branches, maybe-no-member, too-many-statements, unexpected-keyword-arg

"""
.. Note::
   The following script description was last updated on October 24, 2017.

The production script,
located at ``dynamo_consistency/prod/compare.py`` at the time of writing,
goes through the following steps for each site.

  #. Checks that the site status is set to ``'ready'`` in the dynamo database
  #. It gathers the site tree by calling
     :py:func:`dynamo_consistency.getsitecontents.get_site_tree()`.
  #. It gathers the inventory tree by calling
     :py:func:`dynamo_consistency.getinventorycontents.get_db_listing()`.
  #. Creates a list of datasets to not report missing files in.
     This list consists of the following.

     - Deletion requests fetched from PhEDEx by
       :py:func:`dynamo_consistency.checkphedex.set_of_deletions()`

  #. It creates a list of datasets to not report orphans in.
     This list consists of the following.

     - Deletion requests fetched from PhEDEx (same list as datasets to skip in missing)
     - Datasets that have any files on the site, as listed by the dynamo MySQL database
     - Any datasets that have the status flag set to ``'IGNORED'`` in the dynamo database
     - Merging datasets that are
       `protected by Unified <https://cmst2.web.cern.ch/cmst2/unified/listProtectedLFN.txt>`_

  #. Does the comparison between the two trees made,
     using the configuration options listed under
     :ref:`consistency-config-ref` concerning file age.
  #. If the number of missing files is less than **MaxMissing**,
     the number of orphans is less than **MaxOrphan**,
     and the site is under the webpage's "Debugged sites" tab,
     connects to a dynamo registry to report the following errors:

     - For each missing file, every possible source site as listed by the dynamo database,
       (not counting the site where missing), is entered in the transfer queue.
     - Every orphan file and every empty directory that is not too new
       nor should contain missing files is entered in the deletion queue.

  #. Creates a text file that contains the missing blocks and groups.
  #. Creates a text file full of files that only exist elsewhere on tape.
  #. ``.txt`` file lists and details of orphan and missing files are moved to the web space
     and the stats database is updated.

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

import ListDeletable

from dynamo_consistency import config

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
from dynamo_consistency import getinventorycontents
from dynamo_consistency import datatypes
from dynamo_consistency import checkphedex

from CMSToolBox.webtools import get_json
from common.interface.mysql import MySQL

LOG = logging.getLogger(__name__)


def get_registry():
    """
    The connection returned by this must be closed by the caller
    :returns: A connection to the registry database.
    :rtype: :py:class:`common.interface.mysql.MySQL`
    """
    if os.environ['USER'] == 'dynamo':
        return MySQL(config_file='/etc/my.cnf',
                     db='dynamoregister', config_group='mysql-dynamo')
    return MySQL(config_file=os.path.join(os.environ['HOME'], 'my.cnf'),
                 db='dynamoregister', config_group='mysql-register-test')


class EmptyRemover(object):
    """
    This class handles the removal of empty directories from the tree
    by behaving as a callback.
    :param str site: Site name
    :param function check: The function to check against orphans to not delete
    """

    def __init__(self, site, check):
        self.site = site
        self.check = check
        self.removed = 0

    def __call__(self, tree):
        """
        Removes acceptable empty directories from the tree
        :param tree: The tree that is periodically cleaned by this
        :type tree: :py:class:`datatypes.DirectoryInfo`
        """
        tree.setup_hash()
        reg_sql = get_registry()
        for empty in tree.empty_nodes_list():
            full = '/store/' + empty
            # We can prevent most warnings just by checking the length
            if len(full.split('/')) > 6 and not self.check(full):
                self.removed += 1
                LOG.info('Removing directory %s', full)
                tree.remove_node(empty)
                reg_sql.query(
                    """
                    INSERT IGNORE INTO `deletion_queue`
                    (`file`, `site`, `status`) VALUES
                    (%s, %s, 'new')
                    """, full, self.site)

        reg_sql.close()

    def get_removed_count(self):
        """
        :returns: The number of directories removed by this function object
        :rtype: int
        """
        return self.removed


def clean_unmerged(site):
    """
    Lists the /store/unmerged area of a site, and then uses :ref:`unmerged-ref`
    to list files to delete and adds them to the registry.

    ..Warning::

      This function has a number of side effects to various module configurations.
      Definitely call this after running the main site consistency.

    :param str site: The site to run the check over
    :returns: The number of files entered into the register
    :rtype: int
    """

    ## First, we do a bunch of hacky configuration changes for /store/unmerged

    # Set the directory list to unmerged only
    config.DIRECTORYLIST = ['unmerged']
    # Set the IGNORE_AGE for directories to match the ListDeletable config
    datatypes.IGNORE_AGE = ListDeletable.config.MIN_AGE

    # Get the list of protected directories
    ListDeletable.PROTECTED_LIST = ListDeletable.get_protected()

    # Create a tree structure that will hold the protected directories
    protected_tree = datatypes.DirectoryInfo()

    for directory in ListDeletable.PROTECTED_LIST:
        protected_tree.get_node(directory)

    # And do a listing of unmerged
    site_tree = getsitecontents.get_site_tree(
        site, cache='unmerged',
        callback=EmptyRemover(  # Remove while checking the protected_tree
            site, lambda path: bool(protected_tree.get_node(path, make_new=False)))
        )

    # Setup the config a bit more
    deletion_file = site + ListDeletable.config.DELETION_FILE
    ListDeletable.config.DELETION_FILE = deletion_file

    # Reset the protected list in case the listing took a long time
    ListDeletable.PROTECTED_LIST = ListDeletable.get_protected()
    ListDeletable.PROTECTED_LIST.sort()

    # Only consider things older than four weeks
    ListDeletable.get_unmerged_files = lambda: site_tree.get_files(ListDeletable.config.MIN_AGE)
    # Do the cleaning
    ListDeletable.main()

    reg_sql = get_registry()

    n_files = 0

    # Clear out files listed in the deletion file
    for line in open(deletion_file, 'r'):
        n_files += 1
        reg_sql.query(
            """
            INSERT IGNORE INTO `deletion_queue`
            (`file`, `site`, `status`) VALUES
            (%s, %s, 'new')
            """,
            line.strip(), site)


    reg_sql.close()

    return n_files


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
        curs.query(
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

    inv_tree = getinventorycontents.get_db_listing(site)

    # Create the function to check orphans and missing

    # First, datasets in the deletions queue can be missing
    acceptable_missing = checkphedex.set_of_deletions(site)

    # Orphan files cannot belong to any dataset that should be at the site
    inv_sql = MySQL(config_file='/etc/my.cnf', db='dynamo', config_group='mysql-dynamo')
    acceptable_orphans = set(
        inv_sql.query(
            """
            SELECT datasets.name FROM sites
            INNER JOIN dataset_replicas ON dataset_replicas.site_id=sites.id
            INNER JOIN datasets ON dataset_replicas.dataset_id=datasets.id
            WHERE sites.name=%s
            """,
            site)
        )

    # Orphan files may be a result of deletion requests
    acceptable_orphans.update(acceptable_missing)

    # Ignored datasets will not give a full listing, so they can't be accused of having orphans
    acceptable_orphans.update(
        inv_sql.query('SELECT name FROM datasets WHERE status=%s', 'IGNORED')
        )

    # Do not delete anything that is protected by Unified
    protected_unmerged = get_json('cmst2.web.cern.ch', '/cmst2/unified/listProtectedLFN.txt')
    acceptable_orphans.update(['/%s/%s-%s/%s' % (split_name[4], split_name[3],
                                                 split_name[6], split_name[5]) \
                                   for split_name in \
                                   [name.split('/') for name in protected_unmerged['protected']]
                              ])

    LOG.debug('Acceptable orphans: \n%s\n', '\n'.join(acceptable_orphans))

    ignore_list = config_dict.get('IgnoreDirectories', [])

    def double_check(file_name, acceptable):
        """
        Checks the file name against a list of datasets to not list files from.

        :param str file_name: LFN of the file
        :param set acceptable: Datasets to not list files from
                               (Acceptable orphans or missing)
        :returns: Whether the file belongs to a dataset in the list or not
        :rtype: bool
        """
        LOG.debug('Checking file_name: %s', file_name)

        # Skip over paths that include part of the list of ignored directories
        for pattern in ignore_list:
            if pattern in file_name:
                return True

        split_name = file_name.split('/')

        try:
            return '/%s/%s-%s/%s' % (split_name[4], split_name[3],
                                     split_name[6], split_name[5]) in acceptable
        except IndexError:
            LOG.warning('Strange file name: %s', file_name)
            return True

    check_orphans = lambda x: double_check(x, acceptable_orphans)
    check_missing = lambda x: double_check(x, acceptable_missing)

    inv_sql.close()

    # Reset the DirectoryList for the XRootDLister to run on
    config.DIRECTORYLIST = [directory.name for directory in inv_tree.directories]

    remover = EmptyRemover(site, check_orphans)
    site_tree = getsitecontents.get_site_tree(site, remover)

    # Do the comparison
    missing, m_size, orphan, o_size = datatypes.compare(
        inv_tree, site_tree, '%s_compare' % site,
        orphan_check=check_orphans, missing_check=check_missing)

    LOG.debug('Missing size: %i, Orphan size: %i', m_size, o_size)

    inv_sql = MySQL(config_file='/etc/my.cnf', db='dynamo', config_group='mysql-dynamo')

    # Enter things for site in registry
    reg_sql = get_registry()

    # Determine if files should be entered into the registry

    many_missing = len(missing) > int(config_dict['MaxMissing'])
    many_orphans = len(orphan) > int(config_dict['MaxOrphan'])

    # Get whether or not the site is debugged
    conn = sqlite3.connect(os.path.join(webdir, 'stats.db'))
    curs = conn.cursor()
    curs.execute('SELECT isgood FROM sites WHERE site = ?', (site, ))
    is_debugged = curs.fetchone()[0]
    conn.close()

    if is_debugged and not many_missing and not many_orphans:
        def execute(query, *args):
            """
            Executes the query on the registry and outputs a log message depending on query

            :param str query: The SQL query to execute
            :param args: The arguments to the SQL query
            """

            reg_sql.query(query, *args)

            if 'transfer_queue' in query:
                LOG.info('Copying %s from %s', args[0], args[1])
            elif 'deletion_queue' in query:
                LOG.info('Deleting %s', args[0])

    else:
        if many_missing:
            LOG.error('Too many missing files: %i, you should investigate.', len(missing))

        if many_orphans:
            LOG.error('Too many orphan files: %i out of %i, you should investigate.',
                      len(orphan), site_tree.get_num_files())

        execute = lambda *_: 0

    # Then do entries, if the site is in the debugged status

    def add_transfers(line, sites):
        """
        Add the file into the transfer queue for multiple sites.

        :param str line: The file LFN to transfer
        :param list sites: Sites to try to transfer from
        :returns: Whether or not the entry was a success
        :rtype: bool
        """

        # Don't add transfers if too many missing files
        if line in prev_set or not prev_set:
            for location in sites:
                execute(
                    """
                    INSERT IGNORE INTO `transfer_queue`
                    (`file`, `site_from`, `site_to`, `status`, `reqid`)
                    VALUES (%s, %s, %s, 'new', 0)
                    """,
                    line, location, site)

        return bool(sites)


    # Setup a query for sites, with added condition at the end
    site_query = """
                 SELECT sites.name FROM sites
                 INNER JOIN block_replicas ON sites.id = block_replicas.site_id
                 INNER JOIN files ON block_replicas.block_id = files.block_id
                 WHERE files.name = %s AND sites.name != %s
                 AND sites.status = 'ready'
                 AND block_replicas.is_complete = 1
                 AND group_id != 0
                 {0}
                 """

    # Track files with no sources
    no_source_files = []

    for line in missing:

        # Get sites that are not tape
        sites = inv_sql.query(
            site_query.format('AND sites.storage_type != "mss"'),
            line, site)

        if not add_transfers(line, sites):
            # Track files without disk source
            no_source_files.append(line)

            # Get sites that are tape
            sites = inv_sql.query(
                site_query.format('AND sites.storage_type = "mss"'),
                line, site)

            add_transfers(line, sites)



    # Only get the empty nodes that are not in the inventory tree
    for line in orphan + \
            [empty_node for empty_node in site_tree.empty_nodes_list() \
                 if not inv_tree.get_node('/'.join(empty_node.split('/')[2:]),
                                          make_new=False)]:
        execute(
            """
            INSERT IGNORE INTO `deletion_queue`
            (`file`, `site`, `status`) VALUES
            (%s, %s, 'new')
            """,
            line, site)


    reg_sql.close()


    with open('%s_missing_nosite.txt' % site, 'w') as nosite:
        for line in no_source_files:
            nosite.write(line + '\n')

    # We want to track which blocks missing files are coming from
    track_missing_blocks = defaultdict(
        lambda: {'errors': 0,
                 'blocks': defaultdict(lambda: {'group': '',
                                                'errors': 0}
                                      )
                })

    blocks_query = """
                   SELECT blocks.name, IFNULL(groups.name, 'Unsubscribed') FROM blocks
                   INNER JOIN files ON files.block_id = blocks.id
                   INNER JOIN block_replicas ON block_replicas.block_id = files.block_id
                   INNER JOIN sites ON block_replicas.site_id = sites.id
                   LEFT JOIN groups ON block_replicas.group_id = groups.id
                   WHERE files.name = %s AND sites.name = %s
                   """

    with open('%s_compare_missing.txt' % site, 'r') as input_file:
        for line in input_file:
            split_name = line.split('/')
            dataset = '/%s/%s-%s/%s' % (split_name[4], split_name[3], split_name[6], split_name[5])

            output = inv_sql.query(blocks_query, line.strip(), site)

            if not output:
                LOG.warning('The following SQL statement failed: %s',
                            blocks_query % (line.strip(), site))
                LOG.warning('Most likely cause is dynamo update between the listing and now')
                from_phedex = get_json('cmsweb.cern.ch', '/phedex/datasvc/json/prod/filereplicas',
                                       params={'node': site, 'LFN': line.strip()}, use_cert=True)

                try:
                    output = [(from_phedex['phedex']['block'][0]['name'].split('#')[1],
                               from_phedex['phedex']['block'][0]['replica'][0]['group'])]
                except IndexError:
                    LOG.error('File replica not in PhEDEx either!')
                    LOG.error('Skipping block level report for this file.')
                    continue

            block, group = output[0]

            track_missing_blocks[dataset]['errors'] += 1
            track_missing_blocks[dataset]['blocks'][block]['errors'] += 1
            track_missing_blocks[dataset]['blocks'][block]['group'] = group

    inv_sql.close()

    # Output file with the missing datasets
    with open('%s_missing_datasets.txt' % site, 'w') as output_file:
        for dataset, vals in \
                sorted(track_missing_blocks.iteritems(),
                       key=lambda x: x[1]['errors'],
                       reverse=True):

            for block_name, block in sorted(vals['blocks'].iteritems()):
                output_file.write('%10i    %-17s  %s#%s\n' % \
                                      (block['errors'], block['group'],
                                       dataset, block_name))

    # If there were permissions or connection issues, no files would be listed
    # Otherwise, copy the output files to the web directory
    shutil.copy('%s_missing_datasets.txt' % site, webdir)
    shutil.copy('%s_missing_nosite.txt' % site, webdir)
    shutil.copy('%s_compare_missing.txt' % site, webdir)
    shutil.copy('%s_compare_orphan.txt' % site, webdir)

    unmerged = 0
    # Do the unmerged stuff
    if not config_dict['Unmerged'] or site in config_dict['Unmerged']:
        unmerged = clean_unmerged(site)
        shutil.copy('%s_unmerged.txt' % site, webdir)

    if (os.environ.get('ListAge') is None) and (os.environ.get('InventoryAge') is None):

        # Update the runtime stats on the stats page if the listing settings are not changed
        conn = sqlite3.connect(os.path.join(webdir, 'stats.db'))
        curs = conn.cursor()

        curs.execute('INSERT INTO stats_history SELECT * FROM stats WHERE site=?', (site, ))
        curs.execute(
            """
            REPLACE INTO stats VALUES
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, DATETIME(DATETIME(), "-{0} hours"), ?, ?, ?)
            """.format(5 - is_dst),
            (site, time.time() - start, site_tree.get_num_files(),
             remover.get_removed_count() + site_tree.count_nodes(),
             remover.get_removed_count() + len(site_tree.empty_nodes_list()),
             config_dict.get('NumThreads', config_dict.get('MinThreads', 0)),
             len(missing), m_size, len(orphan), o_size, len(no_source_files),
             site_tree.get_num_files(unlisted=True)), unmerged)

        conn.commit()
        conn.close()

    # Make a JSON file reporting storage usage
    if site_tree.get_num_files():
        storage = {
            'storeageservice': {
                'storageshares': [{
                    'numberoffiles': node.get_num_files(),
                    'path': [os.path.normpath('/store/%s' % subdir)],
                    'timestamp': str(int(time.time())),
                    'totalsize': 0,
                    'usedsize': node.get_directory_size()
                    } for node, subdir in [(site_tree.get_node(path), path) for path in
                                           [''] + [d.name for d in site_tree.directories]]
                                  if node.get_num_files()]
                }
            }

        with open(os.path.join(webdir, '%s_storage.json' % site), 'w') as storage_file:
            json.dump(storage, storage_file)


if __name__ == '__main__':

    LOG.info('About to run over %s', SITES)

    for site_to_check in SITES:
        main(site_to_check)
