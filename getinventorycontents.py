# pylint: disable=import-error, wrong-import-position

"""
This module gets the information from the inventory about a site's contents

:author: Daniel Abercrombie <dabercro@mit.edu>
"""

import os
import sys
import time

###############
# !!! DEVELOPMENT ONLY: This all will be replaced when the code is migrated inside dynamo
#
from CMSToolBox.simplefiletools import load_env
load_env('/local/dabercro/dynamo/etc/profile.d/init.sh')
sys.path.insert(0, '/local/dabercro/dynamo/lib')
from common.inventory import InventoryManager
from common.dataformat import File
#
###############

from . import datatypes
from . import config

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
            for file_replica in replica.dataset.files:
                # Make sure we don't waste time/space on directories we don't compare
                if File.directories[file_replica.directory_id].split('/')[2] in dirs_to_look:
                    add_list.append(
                        (os.path.join(File.directories[file_replica.directory_id],
                                      file_replica.name),
                         int(file_replica.size)))

            tree.add_file_list(add_list)

        tree.setup_hash()

        # Save the list in a cache
        tree.save(cache_location)

    else:
        # Just load the cache if it's good
        tree = datatypes.get_info(cache_location)

    return tree
