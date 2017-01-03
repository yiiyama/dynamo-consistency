"""
This module updates the cache of what PHEDEX thinks is at a site.
"""

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
