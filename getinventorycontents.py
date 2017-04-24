# pylint: disable=import-error

"""
This module gets the information from the inventory about a site's contents

:author: Daniel Abercrombie <dabercro@mit.edu>
"""

import os
import time
import logging

from common.inventory import InventoryManager
from common.dataformat import File

from . import datatypes
from . import config

LOG = logging.getLogger(__name__)

def get_site_inventory(site):
    """ Loads the contents of a site, based on the dynamo inventory

    :param str site: The name of the site to load
    :returns: The file replicas that are supposed to be at a site
    :rtype: DirectoryInfo
    """

    cache_location = os.path.join(config.config_dict()['CacheLocation'],
                                  '%s_inventorylisting.pkl' % site)

    if not os.path.exists(cache_location) or \
            (time.time() - os.stat(cache_location).st_mtime) > \
            config.config_dict().get('InventoryAge', 0) * 24 * 3600:

        tree = datatypes.DirectoryInfo('/store')

        inventory = InventoryManager()
        replicas = inventory.sites[site].dataset_replicas

        # Only look in directories in the configuration file
        dirs_to_look = config.config_dict()['DirectoryList']

        for replica in replicas:
            add_list = []
            inventory.store.load_files(replica.dataset)
            blocks_at_site = [brep.block for brep in replica.block_replicas]
            for file_at_site in replica.dataset.files:
                if file_at_site.block not in blocks_at_site:
                    continue
                # Make sure we don't waste time/space on directories we don't compare
                if File.directories[file_at_site.directory_id].split('/')[2] in dirs_to_look:

                    last_created = replica.last_block_created if brep.is_complete \
                                  else int(time.time())
                    add_list.append((file_at_site.fullpath(), file_at_site.size,
                                     last_created, file_at_site.block.real_name()))

            tree.add_file_list(add_list)

        LOG.info('Got full list. Making hash of contents')
        tree.setup_hash()

        # Save the list in a cache
        tree.save(cache_location)

    else:
        # Just load the cache if it's good
        LOG.info('Loading listing from cache: %s', cache_location)
        tree = datatypes.get_info(cache_location)

    return tree
