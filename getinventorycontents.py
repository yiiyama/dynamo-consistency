"""
This module gets the information from the inventory about a site's contents

:author: Daniel Abercrombie <dabercro@mit.edu>
"""

import sys

from CMSToolBox.simplefiletools import load_env
load_env('/local/dabercro/dynamo/etc/profile.d/init.sh')

sys.path.insert(0, '/local/dabercro/dynamo/lib')

from common.inventory import InventoryManager

def update_cache(site):
    """Updates the cache for a site.

    :param str site: The name of the site whose cache will be checked and updated.
    """

    print site


def get_site_contents(site):
    """Loads the contents of a site, based on PHEDEX's count

    :param str site: The name of the site to load
    :returns: The file replicas that are supposed to be at a site
    :rtype: ???
    """

    update_cache(site)

    return site
