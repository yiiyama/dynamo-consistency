""" Module used to perform Consistency Checks using XRootD.

:author: Daniel Abercrombie <dabercro@mit.edu>
"""

import os
import time

from . import config
from . import datatypes

__all__ = ['config', 'datatypes', 'getsitecontents', 'getinventorycontents']

def cache_tree(config_age, location_suffix):
    """
    A decorator for caching pickle files based on the configuration file.
    It is currently set up to decorate a function that has a single parameter ``site``.

    :param str config_age: The key from the config file to read the max age from
    :param str location_suffix: The ending of the main part of the file name
                                where the cached file is saved.
    :returns: A function that uses the caching configuration
    :rtype: func
    """

    def func_decorator(func):
        """
        :param func func: A function to decorate
        :returns: Fancy function
        :rtype: func
        """

        def do_function(site):
            """
            Does a tree function with a single parameter

            :param str site: The site we want the tree for
            :returns: Requested Directory tree
            :rtype: ConsistencyCheck.datatypes.DirInfo
            """

            cache_location = os.path.join(config.config_dict()['CacheLocation'],
                                          '%s_%s.pkl' % (site, location_suffix))

            if not os.path.exists(cache_location) or \
                    (time.time() - os.stat(cache_location).st_mtime) > \
                    config.config_dict().get(config_age, 0) * 24 * 3600:

                tree = func(site)
                tree.setup_hash()
                tree.save(cache_location)

            else:

                tree = datatypes.get_info(cache_location)

            return tree

        return do_function

    return func_decorator
