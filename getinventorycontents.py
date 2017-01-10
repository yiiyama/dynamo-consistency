#pylint: skip-file

"""
This module gets the information from the inventory about a site's contents

:author: Daniel Abercrombie <dabercro@mit.edu>
"""

import os
import sys

# This all will be replaced when the code is migrated inside dynamo

from CMSToolBox.simplefiletools import load_env

load_env('/local/dabercro/dynamo/etc/profile.d/init.sh')
sys.path.insert(0, '/local/dabercro/dynamo/lib')

from common.inventory import InventoryManager
from common.dataformat import File

from . import datatypes

def get_site_inventory(site):
    """Loads the contents of a site, based on PHEDEX's count

    :param str site: The name of the site to load
    :returns: The file replicas that are supposed to be at a site
    :rtype: DirectoryInfo
    """

    tree = datatypes.DirectoryInfo('/store')

    inventory = InventoryManager()
    replicas = inventory.sites[site].dataset_replicas

    for replica in replicas:
        tree.add_file_list(
            [(os.path.join(File.directories[fi.directory_id], fi.name), int(fi.size)) \
                 for fi in replica.dataset.files])

    return tree
