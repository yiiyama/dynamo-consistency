"""
Module contains the datatypes that are used for storage and comparison

:author: Daniel Abercrombie <dabercro@mit.edu>
"""


import os
import hashlib
import pickle
import logging
import threading
import multiprocessing

from Queue import Empty

from . import config

LOG = logging.getLogger(__name__)
IGNORE_AGE = config.config_dict()['IgnoreAge']
"""
The maximum age, in days, of files and directories to ignore in this check.
This variable should be reset once in a while by deamons that run while an
operator might be adjusting the config.yml.
"""

def create_dirinfo(location, name, filler):
    """ Create the directory information in it's very own thread

    :param str location: For the listing
    :param str name: For the DirectoryInfo constructor
    :param function filler: For the listing
    :returns: A DirectoryInfo
    :rtype: DirectoryInfo
    """

    queue = multiprocessing.Queue()
    processes = []

    def empty_dirinfo(location, name, parent=None, lock=None):
        """ Create empty DirectoryInfo

        :param str location: For the DirectoryInfo
        :param str name: Name of the empty DirectoryInfo
        :param DirectoryInfo parent: The parent node to append the directory to
        :param threading.Lock lock: The lock on the parent
        :returns: The empty DirectoryInfo, list of directories, list of file tuples
        :rtype: tuple (DirectoryInfo, list, list)
        """
        full_path = os.path.join(location, name)
        directories, files = filler(full_path)

        output = DirectoryInfo(name, files=files)
        output_lock = threading.Lock()

        if parent:
            lock.acquire()
            parent.directories.append(output)
            parent.directories.sort()
            lock.release()

        for directory in directories:
            queue.put((full_path, directory, output, output_lock))

        return output

    def run_queue():
        """Runs empty_dirinfo over the queue"""
        running = True

        while running:
            try:
                parameters = queue.get(True, 10)
                empty_dirinfo(*parameters)
            except Empty:
                running = False

        LOG.info('Worker finished...')

    dir_info = empty_dirinfo(location, name)

    for _ in xrange(config.config_dict().get('MaxThreads') or multiprocessing.cpu_count()):
        process = multiprocessing.Process(target=run_queue)
        process.start()
        processes.append(process)

    for process in processes:
        process.join()

    dir_info.setup_hash()
    return dir_info


class DirectoryInfo(object):
    """Stores all of the information of a directory"""

    def __init__(self, name='', to_merge=None,
                 directories=None, files=None):
        """ Create the directory information

        :param str name: The name of the directory
        :param list to_merge: If this is filled, the infos in the
                              list are merged into a master DirectoryInfo.
        :param list directories: List of subdirectories inside the directory
        :param list files: List of tuples containing information about files
                           in the directory.
        """

        if to_merge:
            self.directories = to_merge
            self.files = []

        else:
            self.directories = directories or []
            self.files = [{
                'name': name,
                'size': size,
                'mtime': mtime,
                'hash': hashlib.sha1(
                    '%s %i' % (name, size)
                    ).hexdigest()
                } for name, size, mtime in sorted(files or [])]

        self.name = name
        self.hash = None
        self.oldest = None

    def setup_hash(self):
        """
        Set the hashes and times for this DirectoryInfo
        """

        hasher = hashlib.sha1()
        ages = []

        for directory in self.directories:
            directory.setup_hash()
            hasher.update('%s %s' % (directory.name, directory.hash))
            ages.append(directory.oldest)
        for file_info in self.files:
            hasher.update('%s %s' % (file_info['name'], file_info['hash']))
            ages.append(file_info['mtime'])
            LOG.debug('File included: %s size: %i', file_info['name'], file_info['size'])

        self.oldest = min(ages) if ages else 0
        self.hash = hasher.hexdigest()


    def save(self, file_name):
        """
        Save this DirectoryInfo in a file.

        :param str file_name: is the location to save the file
        """

        with open(file_name, 'w') as outfile:
            pickle.dump(self, outfile)

    def display(self, path=''):
        """
        Print out the contents of this DirectoryInfo

        :param str path: The full path to this DirectoryInfo instance
        """

        if not path:
            path = self.name

        print 'oldest: %i my hash: %s path: %s' % (self.oldest, self.hash, path)
        for file_info in self.files:
            print ('mtime: %i size: %i my hash:%s name: %s' %
                   (file_info['mtime'], file_info['size'],
                    file_info['hash'], file_info['name']))

        for directory in self.directories:
            directory.display(os.path.join(path, directory.name))

    def _grab_first(self, levels=100):
        """ Used for debugging.
        Grabs the subdirectories by the first in the list.

        :param int levels: is the number of levels of directories to bypass
        :returns: The proper DirectoryInfo level
        :rtype: DirectoryInfo
        """

        output = self

        for _ in xrange(levels):
            if output.directories:
                output = output.directories[0]
            else:
                break

        return output


def get_info(file_name):
    """
    Get the DirectoryInfo from a file.

    :param str file_name: is the location of the saved information
    :returns: Saved info
    :rtype: DirectoryInfo
    """

    infile = open(file_name, 'r')
    output = pickle.load(infile)
    infile.close()

    return output
