"""
Tools to get the files located at a site.

.. Warning::

   Must be used on a machine with xrdfs installed.

:author: Daniel Abercrombie <dabercro@mit.edu>
"""


import re
import os
import time
import datetime
import subprocess
import logging
import random
import threading

import XRootD.client

from . import config
from . import datatypes

LOG = logging.getLogger(__name__)

def get_site_tree(site):
    """
    Get the information for a site, either from a cache or from XRootD.

    :param str site: The site name
    :returns: The site information
    :rtype: DirectoryInfo
    """

    # Get the redirector for a site
    # The redirector is used for a double check (not implemented yet...)
    # The redir_list is used for the original listing

    _, gate_list = config.get_redirector(site)
    LOG.debug('Full redirector list: %s', gate_list)

    redir_list = [XRootD.client.FileSystem(gate) for gate in gate_list]

    # Get the primary list of servers to hammer
    primary_list = random.sample(redir_list, (len(redir_list) + 1)/2)
    LOG.debug('Primary redirector list: %s', primary_list)

    # Create the filler function for the DirectoryInfo

    error_code_re = re.compile(r'\[(\!|\d+|FATAL)\]')

    def ls_directory(path, attempts=0, prev_stdout='', failed_list=None):
        """
        Gets the contents of the previously defined redirector at a given path

        :param str path: The full path starting with ``/store/``.
        :param int attempts: The number of previous attempts.
                             If the total number of attempts is more than the
                             NumberOfRetries in the config, give back a new dummy file.
                             The young age should lead to the directory being left alone.
        :param str prev_stdout: stdout from previous attempt
        :param list failed_list: A list of redirectors that have not worked
                                 for the current path.
        :returns: A list of directories and list of file information
        :rtype: tuple
        """

        if path[-1] != '/':
            path += '/'

        track_failed_list = failed_list or []

        LOG.debug('Calling ls_directory with: path=%s, attempts=%i, '
                  'prev_stdout lines=%i, failed_list=%s', path, attempts,
                  len([line for line in prev_stdout.split('\n') if line.strip()]),
                  track_failed_list)

        # Get a redirector. First try the primary list, unless all primaries are in the failed list
        if len(track_failed_list) < len(primary_list):
            in_list = primary_list
        # If retrying, reset the failed list and get redirector
        elif attempts < config.config_dict().get('NumberOfRetries', 0):
            track_failed_list = []
            attempts += 1
            in_list = primary_list
        # Otherwise, use the full list
        else:
            in_list = redir_list

        valid_redirs = [server for server in in_list if server not in track_failed_list]
        LOG.debug('List of valid redirectors: %s', valid_redirs)
        redirector = random.choice(valid_redirs)

        LOG.debug('Using redirector %s', redirector)

        status, dir_list = redirector.dirlist(path, flags=XRootD.client.flags.DirListFlags.STAT)

        directories = []
        files = []

        LOG.debug('Status %s', status)
        LOG.debug('Directory listing %s', dir_list)

        if status.ok:

            for entry in dir_list.dirlist:
                LOG.debug('Entry %s', entry)
                if entry.statinfo.flags & XRootD.client.flags.StatInfoFlags.IS_DIR:
                    directories.append((entry.name.lstrip('/'), entry.statinfo.modtime))
                else:
                    files.append((entry.name.lstrip('/'), entry.statinfo.size, entry.statinfo.modtime))

            LOG.debug('From %s returning %i directories and %i files.',
                      path, len(directories), len(files))

            LOG.debug('OUTPUT:\n%s\n%s', directories, files)

        else:

            track_failed_list.append(redirector)
            if len(track_failed_list) < len(redir_list):
                return '_retry_', (path, attempts, '', track_failed_list)

            else:
                LOG.error('Giving up on listing directory %s', path)
                files.append(('_unlisted_', 0, 0))

        return directories, files


    # Create DirectoryInfo for each directory to search
    directories = [
        datatypes.create_dirinfo('/store/', directory, ls_directory) for \
            directory in config.config_dict().get('DirectoryList', [])
        ]

    # Merge the DirectoryInfo
    info = datatypes.DirectoryInfo(name='/store', to_merge=directories)
    info.setup_hash()

    # Save
    info.save(os.path.join(config.config_dict()['CacheLocation'],
                           '%s_remotelisting.pkl' % site))

    # Return the DirectoryInfo
    return info
