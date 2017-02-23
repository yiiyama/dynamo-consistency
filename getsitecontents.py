# pylint: disable=import-error

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

class XRootDLister(object):
    """
    A class that holds two XRootD connections.
    If the primary connection fails, then a fallback connection is used.
    """

    def __init__(self, primary_door, backup_door):
        """
        Set up the class with two doors.
        """

        self.primary_conn = XRootD.client.FileSystem(primary_door)
        self.backup_conn = XRootD.client.FileSystem(backup_door)
        self.error_re = re.compile(r'\[(\!|\d+|FATAL)\]')

        
    def ls_directory(self, door, path, failed_list=None):
        """
        Gets the contents of the previously defined redirector at a given path

        :param XRootD.client.FileSystem door: The door server to use for the listing
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

        LOG.debug('Listing directory with parameters: %s, %s, %s',
                  door, path, failed_list)

        status, dir_list = door.dirlist(path, flags=XRootD.client.flags.DirListFlags.STAT)

        directories = []
        files = []

        LOG.debug('Directory listing %s', dir_list)

        if dir_list:
            for entry in dir_list.dirlist:
                LOG.debug('Entry %s', entry)
                if entry.statinfo.flags & XRootD.client.flags.StatInfoFlags.IS_DIR:
                    directories.append((entry.name.lstrip('/'), entry.statinfo.modtime))
                else:
                    files.append((entry.name.lstrip('/'), entry.statinfo.size, entry.statinfo.modtime))

            LOG.debug('From %s returning %i directories and %i files.',
                      path, len(directories), len(files))

            LOG.debug('OUTPUT:\n%s\n%s', directories, files)

        if not status.ok:

            LOG.warning('While listing %s: %s', path, status.message)

            error_code = self.error_re.search(status.message).group(1)

            if error_code in ['!', '3005']:
                return '_retry_', (directories, files)

        return directories, files

    def check_retry(self, directories, is_primary=True):

        retry = (isinstance(directories, str) and '_retry_' == directories)

        if retry:
            time.sleep(15)
            door = self.primary_conn if is_primary else self.backup_conn
            door = XRootD.client.FileSystem(door.url.hostname)

        return retry

    def list(self, path):
        """
        Return the directory contents at the given path.

        :param str path:
        :returns: A list of directories and list of file information
        :rtype: tuple
        """

        directories, files = self.ls_directory(self.primary_conn, path)

        if self.check_retry(directories):
            directories, files = self.ls_directory(self.backup_conn, path)

        return directories, files


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

    _, door_list = config.get_redirector(site)
    LOG.debug('Full redirector list: %s', door_list)

    # Create DirectoryInfo for each directory to search
    directories = [
        datatypes.create_dirinfo(
            '/store/', directory, XRootDLister,
            [(prim, back) for prim, back in zip(door_list[0::2], door_list[1::2])]) \
            for directory in config.config_dict().get('DirectoryList', [])
        ]

    # Merge the DirectoryInfo
    info = datatypes.DirectoryInfo(name='/store', to_merge=directories)
    info.setup_hash()

    # Save
    info.save(os.path.join(config.config_dict()['CacheLocation'],
                           '%s_remotelisting.pkl' % site))

    # Return the DirectoryInfo
    return info
