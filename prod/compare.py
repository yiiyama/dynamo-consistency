#! /usr/bin/env python

# pylint: disable=import-error, wrong-import-position, unexpected-keyword-arg, too-complex, too-many-locals, too-many-branches, too-many-statements, maybe-no-member

"""
.. Note::
   The following script description was last updated on April 11, 2018.

The production script,
located at ``dynamo_consistency/prod/compare.py`` at the time of writing,
goes through the following steps for each site.

  #. Points :ref:`config-ref` to the local ``consistency_config.json`` file
  #. Notes the time, and if it's daylight savings time for entry into the summary database
  #. Reads the list of previous missing files, since it requires a file to be missing on multiple
     runs before registering it to be copied
  #. It gathers the inventory tree by calling
     :py:func:`dynamo_consistency.getinventorycontents.get_db_listing()`.
  #. Creates a list of datasets to not report missing files in.
     This list consists of the following.

     - Deletion requests fetched from PhEDEx by
       :py:func:`dynamo_consistency.checkphedex.set_of_deletions()`

  #. It creates a list of datasets to not report orphans in.
     This list consists of the following.

     - Datasets that have any files on the site, as listed by the dynamo MySQL database
     - Deletion requests fetched from PhEDEx (same list as datasets to skip in missing)
     - Any datasets that have the status flag set to ``'IGNORED'`` in the dynamo database
     - Merging datasets that are
       `protected by Unified <https://cmst2.web.cern.ch/cmst2/unified/listProtectedLFN.txt>`_

  #. It gathers the site tree by calling
     :py:func:`dynamo_consistency.getsitecontents.get_site_tree()`.
     The list of orphans is used during the running to filter out empty directories that are
     reported to the registry during the run.
  #. Does the comparison between the two trees made,
     using the configuration options listed under
     :ref:`consistency-config-ref` concerning file age.
  #. If the number of missing files is less than **MaxMissing**,
     the number of orphans is less than **MaxOrphan**,
     and the site is under the webpage's "Debugged sites" tab,
     connects to a dynamo registry to report the following errors:

     - Every orphan file and every empty directory that is not too new
       nor should contain missing files is entered in the deletion queue.
     - For each missing file, every possible source site as listed by the dynamo database,
       (not counting the site where missing), is entered in the transfer queue.
       Creates a text file full of files that only exist elsewhere on tape.

  #. Creates a text file that contains the missing blocks and groups.
  #. ``.txt`` file lists and details of orphan and missing files are moved to the web space
  #. If the site is listed in the configuration under the **Unmerged** list,
     the unmerged cleaner is run over the site:

     - :py:func:`dynamo_consistency.getsitecontents.get_site_tree()` is run again,
       this time only over ``/store/unmerged``
     - Empty directories that are not too new nor
       `protected by Unified <https://cmst2.web.cern.ch/cmst2/unified/listProtectedLFN.txt>`_
       are entered into the deletion queue
     - The list of files is passed through the `Unmerged Cleaner
       <http://cms-comp-ops-tools.readthedocs.io/en/latest/siteadmintoolkit.html#unmerged-cleaner>`_
     - The list of files to delete from `Unmerged Cleaner
       <http://cms-comp-ops-tools.readthedocs.io/en/latest/siteadmintoolkit.html#unmerged-cleaner>`_
       are entered in the deletion queue

  #. The summary database is updated to show the last update on the website


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


def deletion(site, files):
    """
    Enters files into the deletion queue for a site
    :param str site: Site to execute deletion
    :param list files: Full LFNs of files or directories to delete
    :returns: Number of files deleted, in case ``files`` is an rvalue or something
    :rtype: int
    """

    reg_sql = get_registry()
    for path in files:
        path = path.strip()
        LOG.info('Deleting %s', path)
        reg_sql.query(
            """
            INSERT IGNORE INTO `deletion_queue`
            (`file`, `site`, `status`) VALUES
            (%s, %s, 'new')
            """, path, site)
    reg_sql.close()

    return len(files)


class EmptyRemover(object):
    """
    This class handles the removal of empty directories from the tree
    by behaving as a callback.
    :param str site: Site name. If value is ``None``, then don't enter deletions
                     into the registry, but still remove node from tree
    :param function check: The function to check against orphans to not delete.
                           The full path name is passed to the function.
                           If it returns ``True``, the directory is not deleted.
    """

    def __init__(self, site, check=None):
        self.site = site
        self.check = check or (lambda _: False)
        self.removed = 0

    def __call__(self, tree):
        """
        Removes acceptable empty directories from the tree
        :param tree: The tree that is periodically cleaned by this
        :type tree: :py:class:`datatypes.DirectoryInfo`
        """
        tree.setup_hash()
        empties = ['/store/' + empty for empty in tree.empty_nodes_list() \
                       if not self.check('/store/' + empty)]

        not_empty = []

        for path in empties:
            try:
                tree.remove_node(path[7:])
            except datatypes.NotEmpty as msg:
                LOG.warning('While removing %s: %s', path, msg)
                not_empty.append(path)

        for path in not_empty:
            empties.remove(path)

        self.removed += deletion(self.site, empties) if self.site else len(empties)

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
    :returns: The number of files entered into the register and the number that are logs
    :rtype: (int, int)
    """

    ## First, we do a bunch of hacky configuration changes for /store/unmerged

    # Set the directory list to unmerged only
    config.DIRECTORYLIST = ['unmerged']
    # Set the IGNORE_AGE for directories to match the ListDeletable config
    datatypes.IGNORE_AGE = ListDeletable.config.MIN_AGE/(24 * 3600)

    # Get the list of protected directories
    ListDeletable.PROTECTED_LIST = ListDeletable.get_protected()
    ListDeletable.PROTECTED_LIST.sort()

    # Create a tree structure that will hold the protected directories
    protected_tree = datatypes.DirectoryInfo()

    for directory in ListDeletable.PROTECTED_LIST:
        protected_tree.get_node(directory)

    def check_protected(path):
        """
        Determine if the path should be protected or not
        :param str path: full path of directory
        :returns: If the path should be protected
        :rtype: bool
        """

        # If the directory is explicitly protected, of course don't delete it
        if bool(protected_tree.get_node(path, make_new=False)):
            return True

        for protected in ListDeletable.PROTECTED_LIST:
            # If a subdirectory, don't delete
            if path.startswith(protected):
                return True
            # We sorted the protected list, so we don't have to check all of them
            if path < protected:
                break

        return False


    # And do a listing of unmerged
    site_tree = getsitecontents.get_site_tree(
        site, cache='unmerged',
        callback=EmptyRemover(site, check_protected))

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

    config_dict = config.config_dict()

    # Delete the contents of the deletion file and the contents of the log directory that are old
    if site_tree.get_node('unmerged/logs', make_new=False):
        with open(deletion_file, 'a') as d_file:
            d_file.write('\n' + '\n'.join(
                site_tree.get_node('unmerged/logs').get_files(
                    min_age=(int(config_dict['UnmergedLogsAge']) * 24 * 3600),
                    path='/store/unmerged')))

    to_delete = set()
    with open(deletion_file, 'r') as d_file:
        to_delete.update([l.strip() for l in d_file])

    # dump undeleted unmerged files into an SQL database
    conn = sqlite3.connect('%s_protected.db' % site)
    curs = conn.cursor()
    currdir = 'fake_start'
    dirid = 1
    currcontents = []
    curs.execute('CREATE TABLE timestamp (timestamp DATETIME);')
    curs.execute('CREATE TABLE directories (id INT PRIMARY KEY, dirname VARCHAR(511));')
    curs.execute("""
                 CREATE TABLE files (dir INT, file CHAR(63),
                                     FOREIGN KEY(dir) REFERENCES directories(id));
                 """)
    curs.execute("""
                 INSERT INTO timestamp (`timestamp`) VALUES (DATETIME({0}, 'unixepoch'));
                 """.format(site_tree.timestamp))
    for fname in site_tree.get_files():
        if fname in to_delete:
            continue
        if fname.startswith(currdir):
            currcontents.append(os.path.basename(fname))
        else:
            if currcontents:
                curs.execute('INSERT INTO directories (`id`, `dirname`) VALUES (?, ?);',
                             (dirid, currdir))
                curs.executemany('INSERT INTO files (`dir`, `file`) VALUES (?, ?);',
                                 [(dirid, f) for f in currcontents])
                dirid += 1

            currdir = os.path.dirname(fname)
            currcontents = [os.path.basename(fname)]

    if currcontents:
        curs.execute('INSERT INTO directories (`id`, `dirname`) VALUES (?, ?);',
                     (dirid, currdir))
        curs.executemany('INSERT INTO files (`dir`, `file`) VALUES (?, ?);',
                         [(dirid, f) for f in currcontents])

    conn.commit()
    conn.close()

    db_dest = os.path.join(config_dict['WebDir'], '%s_protected.db' % site)
    if os.path.exists(db_dest):
        os.remove(db_dest)
    # Move this over to the web directory
    shutil.move('%s_protected.db' % site, config_dict['WebDir'])

    return deletion(site, to_delete), len([f for f in to_delete if f.strip().endswith('.tar.gz')])


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

    inv_tree = getinventorycontents.get_db_listing(site)

    # Reset the DirectoryList for the XRootDLister to run on
    config.DIRECTORYLIST = [directory.name for directory in inv_tree.directories]

    # Directories too short to be checked shouldn't be deleted yet
    remover = EmptyRemover(site)
    site_tree = getsitecontents.get_site_tree(site, remover)

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
    acceptable_orphans.update(
        ['/%s/%s-%s/%s' % (split_name[4], split_name[3], split_name[6], split_name[5]) \
             for split_name in [name.split('/') for name in protected_unmerged['protected']]])

    # Do not delete files being transferred by Dynamo
    acceptable_orphans.update(
        inv_sql.query(
            """
            SELECT DISTINCT d.`name` FROM `file_subscriptions` AS u
            INNER JOIN `files` AS f ON f.`id` = u.`file_id`
            INNER JOIN `blocks` AS b ON b.`id` = f.`block_id`
            INNER JOIN `datasets` AS d ON d.`id` = b.`dataset_id`
            INNER JOIN `sites` AS s ON s.`id` = u.`site_id`
            WHERE s.`name` = %s AND u.`delete` = 0
            """,
            site
        )
    )

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

    # Do the comparison
    missing, m_size, orphan, o_size = datatypes.compare(
        inv_tree, site_tree, '%s_compare' % site,
        orphan_check=check_orphans, missing_check=check_missing)

    LOG.debug('Missing size: %i, Orphan size: %i', m_size, o_size)

    inv_sql = MySQL(config_file='/etc/my.cnf', db='dynamo', config_group='mysql-dynamo')

    # Determine if files should be entered into the registry

    many_missing = len(missing) > int(config_dict['MaxMissing'])
    many_orphans = len(orphan) > int(config_dict['MaxOrphan'])

    # Get whether or not the site is debugged
    conn = sqlite3.connect(os.path.join(webdir, 'stats.db'))
    curs = conn.cursor()
    curs.execute('SELECT isgood FROM sites WHERE site = ?', (site, ))
    is_debugged = curs.fetchone()[0]
    conn.close()

    # Track files with no sources
    no_source_files = []
    unrecoverable = []

    if is_debugged and not many_missing and not many_orphans:
        # Only get the empty nodes that are not in the inventory tree
        deletion(site,
                 orphan + [empty_node for empty_node in site_tree.empty_nodes_list() \
                               if not inv_tree.get_node('/'.join(empty_node.split('/')[2:]),
                                                        make_new=False)]
                )

        # Enter things for site in registry
        reg_sql = get_registry()

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

        for line in missing:

            # Get sites that are not tape
            sites = inv_sql.query(
                site_query.format('AND sites.storage_type != "mss"'),
                line, site)

            if not sites:
                no_source_files.append(line)
                sites = inv_sql.query(
                    site_query.format('AND sites.storage_type = "mss"'),
                    line, site)
                # If still no sites, we are not getting this file back
                if not sites:
                    unrecoverable.append(line)

            # Don't add transfers if too many missing files
            if line in prev_set or not prev_set:
                for location in sites:
                    reg_sql.query(
                        """
                        INSERT IGNORE INTO `transfer_queue`
                        (`file`, `site_from`, `site_to`, `status`, `reqid`)
                        VALUES (%s, %s, %s, 'new', 0)
                        """,
                        line, location, site)

                    LOG.info('Copying %s from %s', line, location)

        reg_sql.close()

    else:
        if many_missing:
            LOG.error('Too many missing files: %i, you should investigate.', len(missing))

        if many_orphans:
            LOG.error('Too many orphan files: %i out of %i, you should investigate.',
                      len(orphan), site_tree.get_num_files())



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

    with open('%s_unrecoverable.txt' % site, 'w') as output_file:
        output_file.write('\n'.join(unrecoverable))

    # If there were permissions or connection issues, no files would be listed
    # Otherwise, copy the output files to the web directory
    shutil.copy('%s_missing_datasets.txt' % site, webdir)
    shutil.copy('%s_missing_nosite.txt' % site, webdir)
    shutil.copy('%s_compare_missing.txt' % site, webdir)
    shutil.copy('%s_compare_orphan.txt' % site, webdir)
    shutil.copy('%s_unrecoverable.txt' % site, webdir)

    unmerged = 0
    unmergedlogs = 0
    # Do the unmerged stuff
    if (not config_dict['Unmerged'] or site in config_dict['Unmerged']) and \
            (os.environ.get('ListAge') is None) and (os.environ.get('InventoryAge') is None):
        unmerged, unmergedlogs = clean_unmerged(site)
        shutil.copy('%s_unmerged.txt' % site, webdir)

    if (os.environ.get('ListAge') is None) and (os.environ.get('InventoryAge') is None):

        # Update the runtime stats on the stats page if the listing settings are not changed
        conn = sqlite3.connect(os.path.join(webdir, 'stats.db'))
        curs = conn.cursor()

        unlisted = site_tree.get_unlisted()

        curs.execute('INSERT INTO stats_history SELECT * FROM stats WHERE site=?', (site, ))
        curs.execute(
            """
            REPLACE INTO stats
            (`site`, `time`, `files`, `nodes`, `emtpy`, `cores`, `missing`, `m_size`,
             `orphan`, `o_size`, `entered`, `nosource`, `unlisted`, `unmerged`, `unlisted_bad`,
             `unrecoverable`, `unmergedlogs`)
            VALUES
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, DATETIME(DATETIME(), "-{0} hours"), ?, ?, ?, ?, ?, ?)
            """.format(5 - is_dst),
            (site, time.time() - start, site_tree.get_num_files(),
             remover.get_removed_count() + site_tree.count_nodes(),
             remover.get_removed_count() + len(site_tree.empty_nodes_list()),
             config_dict.get('NumThreads', config_dict.get('MinThreads', 0)),
             len(missing), m_size, len(orphan), o_size, len(no_source_files),
             len(unlisted), unmerged,
             len([d for d in unlisted \
                      if True not in [i in d for i in config_dict['IgnoreDirectories']]]),
             len(unrecoverable), unmergedlogs
            )
        )

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
