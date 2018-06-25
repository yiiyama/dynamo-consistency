# pylint: disable=import-error, protected-access

"""
This module gets the information from the inventory about a site's contents

:author: Daniel Abercrombie <dabercro@mit.edu>
"""

import time
import bisect
import logging

from dynamo.core.executable import inventory
from dynamo.dataformat import Dataset

from . import datatypes
from . import config
from . import cache_tree

LOG = logging.getLogger(__name__)

# NOTE Is this function used anywhere?
@cache_tree('InventoryAge', 'inventorylisting')
def get_site_inventory(site):
    """ Loads the contents of a site, based on the dynamo inventory

    :param str site: The name of the site to load
    :returns: The file replicas that are supposed to be at a site
    :rtype: dynamo_consistency.datatypes.DirectoryInfo
    """

    tree = datatypes.DirectoryInfo('')

    # Only look in directories in the configuration file
    # Use a sorted list + bisect to reduce the number of comparisons we need to make
    dirs_to_look = sorted(config.config_dict()['DirectoryList'])

    def in_dirs_to_look(lfn):
        i = bisect.bisect_right(dirs_to_look, lfn)
        return lfn.startswith(dirs_to_look[i - 1])

    for replica in inventory.sites[site].dataset_replicas():
        add_list = []

        for block_replica in replica.block_replicas:
            for file_at_site in block_replica.files():
                # Make sure we don't waste time/space on directories we don't compare
                if not in_dirs_to_look(file_at_site.lfn):
                    continue

                last_created = replica.last_block_created if replica.is_complete() \
                              else int(time.time())
                add_list.append((file_at_site.lfn, file_at_site.size,
                                 last_created, file_at_site.block.real_name()))

        tree.add_file_list(add_list)

    return tree


def set_of_ignored():
    """
    Get the full list of IGNORED datasets from the inventory
    """
    inv = INV.get_inventory()
    ignored = set()

    for dataset, details in inv.datasets.iteritems():
        if details.status == Dataset.STAT_IGNORED:
            ignored.add(dataset)

    return ignored


@cache_tree('InventoryAge', 'mysqllisting')
def get_db_listing(site):
    """
    Get the list of files from dynamo database directly from MySQL.

    :param str site: The name of the site to load
    :returns: The file replicas that are supposed to be at a site
    :rtype: dynamo_consistency.datatypes.DirectoryInfo
    """
    LOG.info('About to get the list of for files at %s', site)

    dirs_to_look = sorted(config.config_dict()['DirectoryList'])

    def in_dirs_to_look(lfn):
        i = bisect.bisect_right(dirs_to_look, lfn)
        return lfn.startswith(dirs_to_look[i - 1])

    tree = datatypes.DirectoryInfo('')

    for replica in inventory.sites[site].dataset_replicas():
        # files in a block are typically under a common directory tree - add them to directory info by bulk
        dataset_file_list = []

        for block_replica in replica.block_replicas:
            if block_replica.is_complete() and block_replica.group.name != None:
                timestamp = 0
            else:
                timestamp = int(time.time())

            for file_at_site in block_replica.files():
                lfn = file_at_site.lfn
                if in_dirs_to_look(lfn):
                    dataset_file_list.append((lfn, file_at_site.size, timestamp))

        tree.add_file_list(dataset_file_list)

    return tree
