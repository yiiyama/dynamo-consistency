"""Small module to get information from the config.

:author: Daniel Abercrombie <dabercro@mit.edu>
"""


import os
import time
import subprocess
import logging

import yaml

from CMSToolBox.siteinfo import get_domain

LOG = logging.getLogger(__name__)
CONFIG_FILE = 'config.yml'
"""
The string giving the location of the configuration YAML file.
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
                                'test/config.yml')

    # If file exists, load it
    if os.path.exists(location):
        with open(location, 'r') as config:
            LOG.debug('Opening config: %s', location)
            output = yaml.load(config)
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
    Get the redirector for a given site

    :param str site: The site we want to contact
    :returns: Public hostname of the redirector
    :rtype: str
    """
    config = config_dict()

    # If the redirector is hardcoded, return it
    hard_coded = config.get('Redirectors', {}).get(site)
    if hard_coded:
        return hard_coded

    # Otherwise check the cache
    file_name = os.path.join(config['CacheLocation'], 'redirector_list.txt')

    # Update, if necessary (File doesn't exist or is too old)
    if not os.path.exists(file_name) or \
            (time.time() - os.stat(file_name).st_mtime) > \
            config.get('RedirectorAge', 0) * 24 * 3600:

        with open(file_name, 'w') as redir_file:

            for global_redir in ['xrootd-redic.pi.infn.it', 'cmsxrootd1.fnal.gov']:
                # Get the locate from each redirector
                proc = subprocess.Popen(['xrdfs', global_redir, 'locate', '-h', '/store/'],
                                        stdout=subprocess.PIPE)

                for line in proc.stdout:
                    redir_file.write(line)

                proc.communicate()

    # Parse for a correct redirector
    domain = get_domain(site)
    with open(file_name, 'r') as redir_file:
        for line in redir_file:
            if domain in line:
                return line.split(':')[0]

    # Return blank string if redirector not found
    return ''
