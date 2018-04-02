# pylint: disable=too-complex

"""
Tool to get the files located at a site.

.. Warning::

   Must be used on a machine with XRootD python module installed.

:author: Daniel Abercrombie <dabercro@mit.edu> \n
         Max Goncharov <maxi@mit.edu>
"""

import os
import re
import logging
import random
import itertools
import time
import subprocess
from datetime import datetime

import timeout_decorator

import XRootD.client
from common.interface.mysql import MySQL

from . import config
from . import datatypes
from . import cache_tree


LOG = logging.getLogger(__name__)

def ct_timestamp(line):
    """
    Takes a time string from gfal and extracts the time since epoch

    .. todo::
      Make this more elegant and inline in the :py:func:`GFallDLister.ls_directory`

    :param str line: The line from the gfal-ls call including month, day, and year
                     in some format with lots of hypens
    :returns: Timestamp's time since epoch
    :rtype: int
    """

    fields = line.split('-')
    if ':' in fields[-1]:
        fields[-1] = datetime.now().year
    month = time.strptime(fields[0], '%b').tm_mon
    datestr = str(month) + '-' + str(fields[1]) + '-' + str(fields[2])

    epoch = int(time.mktime(time.strptime(datestr, '%m-%d-%Y')))
    return epoch

class GFallDLister(object):
    """
    An object to list a site through ``gfal-ls`` calls
    """

    def __init__(self, site):
        config_dict = config.config_dict()

        self.store_prefix = config_dict.get('PathPrefix', {}).get(site, '')
        self.tries = config_dict.get('Retries', 0) + 1
        self.ignore_list = config_dict.get('IgnoreDirectories', [])
        self.site = site

        self.log = logging.getLogger(__name__)

        self.mysql_reg = MySQL(config_file='/etc/my.cnf', db='dynamo', config_group='mysql-dynamo')
        sqlline = "select backend from sites where name='" + site + "'"
        self.backend = (self.mysql_reg.query(sqlline))[0]

    def ls_directory(self, path):
        """
        Gets the contents of a path

        :param str path: The full path, starting with ``/store/``, of the directory to list.
        :returns: A bool indicating the success, a list of directories, and a list of files.
        :rtype: bool, list, list
        """

        directories = []
        files = []

        full_path = self.backend + '/' + str(path)

        cmd = 'gfal-ls -l ' + full_path
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   bufsize=4096, shell=True)
        strout, error = process.communicate()
        if process.returncode != 0:
            print error
            return False, directories, files

        for line in strout.split('\n'):
            fields = line.strip().split()
            if len(fields) < 1:
                continue
            item_name = fields[-1]
            item_size = int(fields[4])
            tstamp = ct_timestamp(fields[5] + '-' + fields[6] + '-' + fields[7])

            if fields[0].startswith('d'):
                directories.append((item_name, tstamp))
            else:
                files.append((item_name, item_size, tstamp))

        return True, directories, files

    def list(self, path, retries=1):
        """
        Return the directory contents at the given path.
        The ``list`` member is expected of every object passed to :py:mod:`datatypes`.

        :param str path: The full path, starting with ``/store/``, of the directory to list.
        :param int retries: Number of attempts so far
        :returns: A bool indicating the success, a list of directories, and a list of files.
                  The list of directories consists of tuples of (directory name, mod time).
                  The list of files consistents of tuples of (file name, size, mod time).
                  The modification times are in seconds from epoch and the file size is in bytes.
        :rtype: bool, list, list
        """

        # Skip over paths that include part of the list of ignored directories
        for pattern in self.ignore_list:
            if pattern in path:
                return True, [], []

        if retries == self.tries:
            self.log.error('Giving up on %s due to too many retries', path)
            return False, [], []

        try:
            okay, directories, files = self.ls_directory(path)
             # We could add sleep, reconnecting and other error handling here, if desired
            if not okay:
                okay, directories, files = self.list(path, retries + 1)

        except timeout_decorator.TimeoutError:
            self.log.warning('Directory %s timed out.', path)
            okay, directories, files = self.list(path, retries + 1)

        return okay, directories, files


class XRootDLister(object):
    """
    A class that holds two XRootD connections.
    If the primary connection fails to list a directory,
    then a fallback connection is used.
    This keeps the load of listing from hitting more than half
    of a site's doors at a time.

    :param str primary_door: The URL of the door that will get the most load
    :param str backup_door: The URL of the door that will be used when
                            the primary door fails
    :param str site: The site that this connection is to.
    :param int thread_num: This optional parameter is only used to
                           Create a separate logger for this object
    :param bool do_both: If true, the primary and backup doors will both
                         be used for every listing
    """

    def __init__(self, primary_door, backup_door, site, thread_num=None, do_both=False):
        config_dict = config.config_dict()

        self.primary_conn = XRootD.client.FileSystem(primary_door)
        self.backup_conn = XRootD.client.FileSystem(backup_door)
        self.do_both = do_both

        self.store_prefix = config_dict.get('PathPrefix', {}).get(site, '')
        self.access = config_dict.get('AccessMethod', {})
        self.tries = config_dict.get('Retries', 0) + 1
        self.ignore_list = config_dict.get('IgnoreDirectories', [])
        self.site = site

        if thread_num is None:
            self.log = logging.getLogger(__name__)
        else:
            self.log = logging.getLogger('%s--thread%i' % (__name__, thread_num))

        self.log.info('Connections created at %s (primary) and %s (backup)',
                      primary_door, backup_door)

    @timeout_decorator.timeout(config.config_dict()['Timeout'], use_signals=True)
    def ls_directory(self, door, path):
        """
        Gets the contents of the previously defined redirector at a given path

        :param XRootD.client.FileSystem door: The door server to use for the listing
        :param str path: The full path, starting with ``/store/``, of the directory to list.
        :returns: A bool indicating the success, a list of directories, and a list of files.
                  See :py:func:`XRootDLister.list` for more details on the output.
        :rtype: bool, list, list
        """

        # If there is a prefix, prepend that
        if self.store_prefix:
            path = os.path.normpath(os.path.sep.join([self.store_prefix, path]))

        # FileSystem only works with ending slashes for some sites (not all, but let's be safe)
        if path[-1] != '/':
            path += '/'

        self.log.debug('Using door at %s to list directory %s', door.url, path)

        if self.site in self.access and self.access[self.site] == 'directx':
            return self.direct_list(door, path)

        # http://xrootd.org/doc/python/xrootd-python-0.1.0/modules/client/filesystem.html#XRootD.client.FileSystem.dirlist
        status, dir_list = door.dirlist(path, flags=XRootD.client.flags.DirListFlags.STAT)

        directories = []
        files = []

        self.log.debug('For %s, directory listing good: %s', path, bool(dir_list))

        # Assumes the listing went well for now
        okay = True

        # If there's a directory listing, parse it
        if dir_list:
            for entry in dir_list.dirlist:
                if entry.statinfo.flags & XRootD.client.flags.StatInfoFlags.IS_DIR:
                    directories.append((entry.name.lstrip('/'), entry.statinfo.modtime))
                else:
                    files.append((entry.name.lstrip('/'), entry.statinfo.size,
                                  entry.statinfo.modtime))

        # If status isn't perfect, analyze the error
        if not status.ok:

            self.log.warning('While listing %s: %s', path, status.message)
            ok = False

            self.log.debug('Error code: %s', error_code)
            self.log.debug('Directory List: %s', dir_list)
            self.log.debug('Okay: %i', okay)

        self.log.debug('From %s returning status %i with %i directories and %i files.',
                       path, okay, len(directories), len(files))

        return okay, directories, files

    @staticmethod
    def direct_list(door, path):
        """
        Do the listing using system calls

        :param XRootD.client.FileSystem door: The door server to use for the listing
        :param str path: The full path, starting with ``/store/``, of the directory to list.
        :returns: A bool indicating the success, a list of directories, and a list of files.
                  See :py:func:`XRootDLister.list` for more details on the output.
        :rtype: bool, list, list
        """

        directories = []
        files = []

        doorname = str(door.url.hostname) + ':' + str(door.url.port)
        cmd = 'xrdfs ' + doorname + ' ls -l ' + path
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   bufsize=4096, shell=True)
        strout, error = process.communicate()
        if process.returncode != 0:
            print error
            return False, directories, files

        for line in strout.split('\n'):
            fields = line.strip().split()
            if len(fields) < 1:
                continue
            item_name = os.path.basename(fields[-1].rstrip('/'))
            item_size = int(fields[3])
            tstamp = int(time.mktime(time.strptime(fields[1], '%Y-%m-%d')))

            if fields[0].startswith('d'):
                directories.append((item_name, tstamp))
            else:
                files.append((item_name, item_size, tstamp))

        return True, directories, files

    def list(self, path, retries=0):
        """
        Return the directory contents at the given path.
        The ``list`` member is expected of every object passed to :py:mod:`datatypes`.

        :param str path: The full path, starting with ``/store/``, of the directory to list.
        :param int retries: Number of attempts so far
        :returns: A bool indicating the success, a list of directories, and a list of files.
                  The list of directories consists of tuples of (directory name, mod time).
                  The list of files consistents of tuples of (file name, size, mod time).
                  The modification times are in seconds from epoch and the file size is in bytes.
        :rtype: bool, list, list
        """

        # Skip over paths that include part of the list of ignored directories
        for pattern in self.ignore_list:
            if pattern in path:
                return True, [], []

        if retries == self.tries:
            self.log.error('Giving up on %s due to too many retries', path)
            return False, [], []

        try:
            # Try with primary door
            okay, directories, files = self.ls_directory(self.primary_conn, path)

            # We could add sleep, reconnecting and other error handling here, if desired
            if not okay:
                # Try with backup door
                okay, directories, files = self.ls_directory(self.backup_conn, path)

                okay &= (not self.do_both)
            elif self.do_both:

                okay_back, directories_back, files_back = \
                    self.ls_directory(self.backup_conn, path)

                okay &= okay_back
                directories = list(set(directories + directories_back))
                files = list(set(files + files_back))

        except timeout_decorator.TimeoutError:
            self.log.warning('Directory %s timed out.', path)

            # Reconnect

            # First try to get the list of doors that are not connected in this instance
            _, door_list = config.get_redirector(
                self.site,
                list(
                    itertools.chain.from_iterable(
                        [re.search(r'root://((.*):\d*)/', str(conn.url)).group(1, 2) for conn in \
                             [self.primary_conn, self.backup_conn]]
                        )
                    )
                )

            # If we get any, reconnect
            if door_list:

                if len(door_list) == 1:
                    door_list.extend(door_list)

                use_doors = random.sample(door_list, 2)

                self.primary_conn = XRootD.client.FileSystem(use_doors[0])
                self.backup_conn = XRootD.client.FileSystem(use_doors[1])

            # Otherwise, swap urls
            else:
                backup_url = str(self.backup_conn.url)
                self.primary_conn = XRootD.client.FileSystem(backup_url)
                self.backup_conn = XRootD.client.FileSystem(str(self.primary_conn.url))

            return self.list(path, retries + 1)

        if not okay and not self.store_prefix and len(path.strip('/').split('/')) < 4:

            # Try to fall back on /cms
            self.store_prefix = '/cms'
            self.log.warning('Trying to fall back to using suffix %s', self.store_prefix)
            okay, directories, files = self.list(path)
            if not okay:
                self.log.warning('Fallback did not work, reverting')
                self.store_prefix = ''

        return okay, directories, files


@cache_tree('ListAge', 'remotelisting')
def get_site_tree(site, callback=None):
    """
    Get the information for a site, from XRootD or a cache.

    :param str site: The site name
    :param function callback: The callback function to pass to :py:func:`datatypes.create_dirinfo`
    :returns: The site directory listing information
    :rtype: dynamo_consistency.datatypes.DirectoryInfo
    """

    config_dict = config.config_dict()
    access = config_dict.get('AccessMethod', {})
    if access.get(site) == 'SRM':
        num_threads = int(config_dict.get('GFALThreads'))
        LOG.info('threads = %i', num_threads)
        directories = [
            datatypes.create_dirinfo('/store', directory, GFallDLister,
                                     [[site]]*num_threads, callback) \
                for directory in config.config_dict().get('DirectoryList', [])
        ]
        # Return the DirectoryInfo
        return datatypes.DirectoryInfo(name='/store', directories=directories)


    # Get the redirector for a site
    # The redirector can be used for a double check (not implemented yet...)
    # The redir_list is used for the original listing
    num_threads = int(config_dict.get('NumThreads'))

    balancer, door_list = config.get_redirector(site)
    LOG.debug('Full redirector list: %s', door_list)

    # Bool to determine if using both doors in each connection
    do_both = bool(site in config_dict.get('BothList', []))

    if site in config_dict.get('UseLoadBalancer', []) or \
            (balancer and not door_list):
        num_threads = 1
        door_list = [balancer]

    if not door_list:
        LOG.error('No doors found. Returning emtpy tree')
        return datatypes.DirectoryInfo(name='/store')

    while num_threads > (len(door_list) + 1)/2:
        if do_both or len(door_list) % 2:
            door_list.extend(door_list)
        else:
            # If even number of redirectors and not using both, stagger them
            door_list.extend(door_list[1:])
            door_list.append(door_list[0])

    # Add the first door to the end, in case we have an odd number of doors
    door_list.append(door_list[0])
    # Strip off the extra threads
    door_list = door_list[:num_threads * 2]

    # Create DirectoryInfo for each directory to search (set in configuration file)
    # The search is done with XRootDLister objects that have two doors and the thread
    # number as initialization arguments.

    directories = [
        datatypes.create_dirinfo(
            '/store/', directory, XRootDLister,
            [(prim, back, site, thread_num, do_both) for prim, back, thread_num in \
                 zip(door_list[0::2], door_list[1::2], range(len(door_list[1::2])))],
            callback) \
            for directory in config_dict.get('DirectoryList', [])
        ]

    # Return the DirectoryInfo
    return datatypes.DirectoryInfo(name='/store', directories=directories)
