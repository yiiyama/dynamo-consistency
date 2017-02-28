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
import logging

import XRootD.client

from . import config
from . import datatypes

LOG = logging.getLogger(__name__)

class XRootDLister(object):
    """
    A class that holds two XRootD connections.
    If the primary connection fails, then a fallback connection is used.
    """

    def __init__(self, primary_door, backup_door, thread_num=0):
        """
        Set up the class with two doors.
        """

        self.primary_conn = XRootD.client.FileSystem(primary_door)
        self.backup_conn = XRootD.client.FileSystem(backup_door)
        self.error_re = re.compile(r'\[(\!|\d+|FATAL)\]')
        self.log = logging.getLogger('%s--thread%i' % (__name__, thread_num))

    def ls_directory(self, door, path):
        """
        Gets the contents of the previously defined redirector at a given path

        :param XRootD.client.FileSystem door: The door server to use for the listing
        :param str path: The full path starting with ``/store/``.
        :returns: A list of directories and list of file information
        :rtype: tuple
        """

        if path[-1] != '/':
            path += '/'

        self.log.debug('Using door at %s to list directory %s', door.url.hostname, path)

        status, dir_list = door.dirlist(path, flags=XRootD.client.flags.DirListFlags.STAT)

        directories = []
        files = []

        self.log.debug('For %s, directory listing good? %i', path, bool(dir_list))

        ok = True

        if dir_list:
            for entry in dir_list.dirlist:
                if entry.statinfo.flags & XRootD.client.flags.StatInfoFlags.IS_DIR:
                    directories.append((entry.name.lstrip('/'), entry.statinfo.modtime))
                else:
                    files.append((entry.name.lstrip('/'), entry.statinfo.size,
                                  entry.statinfo.modtime))

        if not status.ok:

            self.log.warning('While listing %s: %s', path, status.message)

            error_code = self.error_re.search(status.message).group(1)

            if error_code in ['!', '3005', '3010']:
                ok = bool(dir_list)

        self.log.debug('From %s returning status %i with %i directories and %i files.',
                  path, ok, len(directories), len(files))

        return ok, directories, files

    def check_retry(self, ok, is_primary=True):
        """
        Check the output of the directory listing to determine whether
        or not to retry with the backup door.

        :param bool ok: This is the first output from the directory listing
                        which is used to determine whether or not to retry
        :param bool is_primary: says if the door last listed is the primary or backup door
        :returns: whether or not to retry the listing
        :rtype: bool
        """

        retry = not ok

        if retry:
            time.sleep(10)

        return retry

    def list(self, path):
        """
        Return the directory contents at the given path.

        :param str path:
        :returns: A list of directories and list of file information
        :rtype: tuple
        """

        ok, directories, files = self.ls_directory(self.primary_conn, path)

        if self.check_retry(ok):
            ok, directories, files = self.ls_directory(self.backup_conn, path)

        return ok, directories, files


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
            [(prim, back, thread_num) for prim, back, thread_num in \
                 zip(door_list[0::2], door_list[1::2], range(len(door_list[1::2])))]) \
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
