"""Small module to get information from the config.

:author: Daniel Abercrombie <dabercro@mit.edu>
"""


import os
import time
import subprocess
import logging
import json
import random

from CMSToolBox.siteinfo import get_domain

LOG = logging.getLogger(__name__)
CONFIG_FILE = 'consistency_config.json'
"""
The string giving the location of the configuration JSON file.
Generally, you want to set this value of the module before calling for your configuration.
"""

def config_dict():
    """
    :returns: the configuration in a dict
    :rtype: str
    :raises IOError: when it cannot find the configuration file
    :raises KeyError: when the CacheDirectory key is not set in the configuration file
    """

    location = CONFIG_FILE
    output = None

    # If not there, fall back to the test directory
    if not os.path.exists(location):
        location = os.path.join(os.path.dirname(__file__),
                                'test', CONFIG_FILE)

    # If file exists, load it
    if os.path.exists(location):
        with open(location, 'r') as config:
            LOG.debug('Opening config: %s', location)
            output = json.load(config)
    else:
        raise IOError('Could not load config at ' + location)

    cache_location = output.get('CacheLocation')

    # Create the directory holding the cache
    if cache_location:
        if not os.path.exists(cache_location):
            os.makedirs(cache_location)
    else:
        raise KeyError('Configuration dictionary does not have a Cache Location set. '
                       'Using dictionary at ' + location)

    return output


def get_redirector(site):
    """
    Get the redirector and xrootd servers for a given site

    :param str site: The site we want to contact
    :returns: Public hostname of the local redirector
              and a list of xrootd servers
    :rtype: str, list
    """
    config = config_dict()

    # If the redirector is hardcoded, return it
    redirector = config.get('Redirectors', {}).get(site, '')

    def dump_file(redirs, file_name):
        """
        Dump the redirector info into a file.

        :param list redirs: Is a list of redirectors to check for servers
        :param str file_name: Is the name of the file to output the redirector
        """

        # Update, if necessary (File doesn't exist or is too old)
        if not os.path.exists(file_name) or \
                (time.time() - os.stat(file_name).st_mtime) > \
                config.get('RedirectorAge', 0) * 24 * 3600:

            with open(file_name, 'w') as redir_file:

                for global_redir in redirs:
                    # Get the locate from each redirector
                    proc = subprocess.Popen(['xrdfs', global_redir, 'locate', '-h', '/store/'],
                                            stdout=subprocess.PIPE)

                    for line in proc.stdout:
                        redir_file.write(line.split(':')[0] + '\n')

                    proc.communicate()


    # If not hard-coded, get the redirector
    if not redirector:
        # Otherwise check the cache
        file_name = os.path.join(config['CacheLocation'], 'redirector_list.txt')

        dump_file(['xrootd-redic.pi.infn.it', 'cmsxrootd1.fnal.gov'], file_name)

        # Parse for a correct redirector
        domain = get_domain(site)
        with open(file_name, 'r') as redir_file:
            for line in redir_file:
                if domain in line:
                    redirector = line.strip()
                    break

    list_name = os.path.join(config['CacheLocation'], '%s_redirector_list.txt' % site)
    dump_file([redirector], list_name)

    local_list = []

    with open(list_name, 'r') as list_file:
        for line in list_file:
            local_list.append(line.strip())

    # Return redirector and list of half the redirectors (rounded up)
    return (redirector, random.sample(local_list, (len(local_list) + 1)/2))
