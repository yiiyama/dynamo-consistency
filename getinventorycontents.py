# pylint: disable=import-error, wrong-import-position

"""
This module gets the information from the inventory about a site's contents

:author: Daniel Abercrombie <dabercro@mit.edu>
"""

import os
import sys

from CMSToolBox.simplefiletools import load_env

# This all will be replaced when the code is migrated inside dynamo

load_env('/local/dabercro/dynamo/etc/profile.d/init.sh')
sys.path.insert(0, '/local/dabercro/dynamo/lib')

from common.inventory import InventoryManager
from common.dataformat import File

from . import datatypes
from . import config

def get_site_inventory(site):
    """ Loads the contents of a site, based on the dynamo inventory

    :param str site: The name of the site to load
    :returns: The file replicas that are supposed to be at a site
    :rtype: DirectoryInfo
    """

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
                    (os.path.join(File.directories[file_replica.directory_id], file_replica.name),
                     int(file_replica.size)))

        tree.add_file_list(add_list)

    tree.setup_hash()

    return tree
