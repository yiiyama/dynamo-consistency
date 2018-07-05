"""
Tool to get the files located at a site.

.. Warning::

   Must be used on a machine with XRootD python module installed.

:author: Daniel Abercrombie <dabercro@mit.edu> \n
         Max Goncharov <maxi@mit.edu>
"""

import re
import logging
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


class Lister(object):
    """
    The protoype of the listing facility

    :param int thread_num: This optional parameter is only used to
                           Create a separate logger for this object
    :param str site: Used for reading the correct configuration
    """
    def __init__(self, thread_num, site):
        config_dict = config.config_dict()
        self.log = logging.getLogger(__name__ if thread_num is None else \
                                         '%s--thread%i' % (__name__, thread_num))
        self.ignore_list = config_dict.get('IgnoreDirectories', [])
        self.store_prefix = config_dict.get('PathPrefix', {}).get(site, '')
        self.tries = config_dict.get('Retries', 0) + 1

        self.fallback_tries = 5


    def ls_directory(self, path):
        """
        Prototype function that lists the directories

        :param str path: The full path, starting with ``/store/``, of the directory to list.
        """
        pass


    def reconnect(self):
        """
        A prototype for reconnecting to the remote servers
        """
        pass


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
                self.log.warning('Ignoring %s because of ignored pattern %s', path, pattern)
                return False, [], []

        if retries >= self.tries:
            self.log.error('Giving up on %s due to too many retries', path)
            return False, [], []

        if retries:
            self.reconnect()

        # FileSystem only works with ending slashes for some sites (not all, but let's be safe)
        path = self.store_prefix + path + ('/' if path[-1] != '/' else '')

        try:
            okay, directories, files = self.ls_directory(path)

            if not okay and self.fallback_tries and \
                    not self.store_prefix and len(path.strip('/').split('/')) < 4:

                self.fallback_tries -= 1

                # Try to fall back on /cms
                self.store_prefix = '/cms'
                self.log.warning('Trying to fall back to using suffix %s', self.store_prefix)
                okay, directories, files = self.list(path, self.tries - 1)

                if not okay:
                    self.log.warning('Fallback did not work, reverting')
                    self.store_prefix = ''

        except timeout_decorator.TimeoutError:
            self.log.warning('Directory %s timed out.', path)
            okay = False

        if not okay:
            okay, directories, files = self.list(path, retries + 1)

        return okay, directories, files


def ct_timestamp(line):
    """
    Takes a time string from gfal and extracts the time since epoch

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


class GFalLister(Lister):
    """
    An object to list a site through ``gfal-ls`` calls
    """

    def __init__(self, site, thread_num=None):
        super(GFalLister, self).__init__(thread_num, site)
        mysql_reg = MySQL(config_file='/etc/my.cnf', db='dynamo', config_group='mysql-dynamo')
        self.backend = mysql_reg.query('SELECT backend FROM sites WHERE name=%s', site)[0]
        mysql_reg.close()


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
            self.log.error(error)
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


class XRootDSubShell(Lister):
    """
    Very similar to the :py:class:`XRootDLister`,
    but uses a subshell through `pexpect`.
    """
    def __init__(self, site, door, thread_num=None):
        super(XRootDSubShell, self).__init__(thread_num, site)

        self.shell = subprocess.Popen(['xrdfs', door],
                                      stdin=subprocess.PIPE,
                                      stdout=subprocess.PIPE,
                                      stderr=subprocess.STDOUT)
        self.uri = door


    def __del__(self):
        self.shell.communicate('quit')


    @timeout_decorator.timeout(config.config_dict()['Timeout'])
    def ls_directory(self, path):

        self.log.debug('Directly listing %s with %s', path, self.uri)

        self.shell.stdin.write('ls -l %s\n' % path)
        self.shell.stdin.write('\n') # send one extra [ENTER] to make an empty prompt appear
        self.shell.stdin.flush()
        self.shell.stdout.readline().strip() # Read out the original prompt line

        directories = []
        files = []
        okay = True

        while True:
            line = self.shell.stdout.readline().strip()

            self.log.debug(line)

            if line.endswith('>'): # stop reading at the empty prompt (otherwise readline hangs)
                break

            # Parse the line
            # First check that it matches the expected format
            match = re.match( # (d)rwx (YYYY-MM-DD HH:MM:SS)     (size) (name)
                r'(d|\-).{3}\s(\d{4}\-\d{2}\-\d{2}\s\d{2}:\d{2}:\d{2})\s*(\d*)\s([^\s]*)',
                line)

            if match:
                # Get the timestamp
                mtime = time.mktime(time.strptime(match.group(2), '%Y-%m-%d %H:%M:%S'))
                # Get the relative name
                name = match.group(4).split('/')[-1]
                if match.group(1) == 'd':  # Directory
                    directories.append((name, mtime))
                else:                    # File
                    files.append((name, int(match.group(3)), mtime))
            elif re.match(r'.*\[\d+\].*', line):
                # Otherwise, we probably have an error on our hands
                okay = False
                self.log.error(line)

        return okay, directories, files



class XRootDLister(Lister):
    """
    A class that holds two XRootD connections.
    If the primary connection fails to list a directory,
    then a fallback connection is used.
    This keeps the load of listing from hitting more than half
    of a site's doors at a time.

    :param str site: The site that this connection is to.
    :param str door: The URL of the door that will get the most load
    :param int thread_num: This optional parameter is only used to
                           Create a separate logger for this object
    """
    def __init__(self, site, door, thread_num=None):
        super(XRootDLister, self).__init__(thread_num, site)

        self.conn = XRootD.client.FileSystem(door)

        self.log.info('Connection created at %s', door)


    @timeout_decorator.timeout(config.config_dict()['Timeout'])
    def ls_directory(self, path):
        """
        Gets the contents of the previously defined redirector at a given path

        :param str path: The full path, starting with ``/store/``, of the directory to list.
        :returns: A bool indicating the success, a list of directories, and a list of files.
        :rtype: bool, list, list
        """

        self.log.debug('Using door at %s to list directory %s', self.conn.url, path)

        # http://xrootd.org/doc/python/xrootd-python-0.1.0/modules/client/filesystem.html#XRootD.client.FileSystem.dirlist
        status, dir_list = self.conn.dirlist(path, flags=XRootD.client.flags.DirListFlags.STAT)

        directories = []
        files = []

        self.log.debug('For %s, directory listing good: %s', path, bool(dir_list))

        # If there's a directory listing, parse it
        if dir_list:
            for entry in dir_list.dirlist:
                if entry.statinfo.flags & XRootD.client.flags.StatInfoFlags.IS_DIR:
                    directories.append((entry.name.lstrip('/'), entry.statinfo.modtime))
                else:
                    files.append((entry.name.lstrip('/'), entry.statinfo.size,
                                  entry.statinfo.modtime))

        okay = bool(status.ok)

        # If status isn't perfect, analyze the error
        if not okay:

            self.log.warning('While listing %s: %s', path, status.message)
            self.log.debug('Directory List: %s', dir_list)
            self.log.debug('Okay: %i', okay)

        self.log.debug('From %s returning status %i with %i directories and %i files.',
                       path, okay, len(directories), len(files))

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
            datatypes.create_dirinfo('/store', directory, GFalLister,
                                     [(site, x) for x in xrange(num_threads)], callback) \
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

    if site in config_dict.get('UseLoadBalancer', []) or \
            (balancer and not door_list):
        num_threads = 1
        door_list = [balancer]

    if not door_list:
        LOG.error('No doors found. Returning emtpy tree')
        return datatypes.DirectoryInfo(name='/store')

    while num_threads > len(door_list):
        if len(door_list) % 2:
            door_list.extend(door_list)
        else:
            # If even number of redirectors and not using both, stagger them
            door_list.extend(door_list[1:])
            door_list.append(door_list[0])

    # Strip off the extra threads
    door_list = door_list[:num_threads]

    # Create DirectoryInfo for each directory to search (set in configuration file)
    # The search is done with XRootDLister objects that have two doors and the thread
    # number as initialization arguments.

    directories = [
        datatypes.create_dirinfo(
            '/store/', directory,
            XRootDSubShell if access.get(site) == 'directx' else XRootDLister,
            [(site, door, thread_num) for thread_num, door in enumerate(door_list)],
            callback) for directory in config_dict.get('DirectoryList', [])
        ]

    # Return the DirectoryInfo
    return datatypes.DirectoryInfo(name='/store', directories=directories)
