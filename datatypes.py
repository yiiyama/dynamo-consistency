# pylint: disable=too-complex
# This will need to be fixed, along with documentation

"""
Module contains the datatypes that are used for storage and comparison

:author: Daniel Abercrombie <dabercro@mit.edu>
"""


import os
import hashlib
import pickle
import logging
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

    in_queue = multiprocessing.Queue()
    out_queue = multiprocessing.Queue()
    processes = []

    dir_info = DirectoryInfo(name)

    def check_dir(location, name):
        """ Checks out a location, and if it has files (or is a dead end)
        places the name of the node in a queue for later processing

        :param str location: For the DirectoryInfo
        :param str name: Name of the empty DirectoryInfo
        """

        full_path = os.path.join(location, name)
        directories, files = filler(full_path)

        if files or not directories:
            out_queue.put((name, files))

        for directory in directories:
            in_queue.put((location, os.path.join(name, directory)))

    check_dir(os.path.join(location, name), '')

    def run_queue():
        """Runs empty_dirinfo over the queue"""
        running = True

        while running:
            try:
                parameters = in_queue.get(True, 3)
                check_dir(*parameters)
            except Empty:
                running = False

        LOG.info('Worker finished input queue')

    for _ in xrange(config.config_dict().get('MaxThreads') or multiprocessing.cpu_count()):
        process = multiprocessing.Process(target=run_queue)
        process.start()
        processes.append(process)

    for process in processes:
        process.join()

    building = True

    while building:
        try:
            name, files = out_queue.get(True, 3)
            dir_info.get_node(name).set_files(files)
        except Empty:
            building = False

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

        else:
            self.directories = directories or []

        self.name = name
        self.hash = None
        self.oldest = None
        self.files = None
        self.set_files(files)

    def set_files(self, files):
        """
        Set the files for this DirectoryInfo node

        :param list files: The tuples of file information
        """
        LOG.debug('Setting %i files', len(files or []))
        self.files = [{
            'name': name,
            'size': size,
            'mtime': mtime,
            'hash': hashlib.sha1(
                '%s %i' % (name, size)
                ).hexdigest()
            } for name, size, mtime in sorted(files or [])]

    def setup_hash(self):
        """
        Set the hashes and times for this DirectoryInfo
        """

        hasher = hashlib.sha1()
        ages = []

        self.directories.sort(key=lambda x: x.name)
        self.files.sort(key=lambda x: x['name'])

        for directory in self.directories:
            directory.setup_hash()
            hasher.update('%s %s' % (directory.name, directory.hash))
            if directory.oldest:
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

        self.setup_hash()

        with open(file_name, 'w') as outfile:
            pickle.dump(self, outfile)

    def display(self, path=''):
        """
        Print out the contents of this DirectoryInfo

        :param str path: The full path to this DirectoryInfo instance
        """

        self.setup_hash()

        if not path:
            path = self.name

        print 'oldest: %i my hash: %s path: %s' % (self.oldest, self.hash, path)
        for file_info in self.files:
            print ('mtime: %i size: %i my hash:%s name: %s' %
                   (file_info['mtime'], file_info['size'],
                    file_info['hash'], file_info['name']))

        for directory in self.directories:
            directory.display(os.path.join(path, directory.name))

    def get_node(self, path):
        """ Get the node that corresponds to the path given

        :param str path: Path to the desired node from current node.
                         If the path does not exist yet, empty nodes will be created.
        :returns: A node with the proper path
        :rtype: DirectoryInfo
        """

        LOG.debug('From node %s named %s, getting %s', self, self.name, path)

        # If any path left
        if path:
            split_path = path.split('/')

            # Search for if directory exists
            for directory in self.directories:
                if split_path[0] == directory.name:
                    return directory.get_node('/'.join(split_path[1:]))

            # If not, make a new directory
            new_dir = DirectoryInfo(split_path[0])
            self.directories.append(new_dir)
            return new_dir.get_node('/'.join(split_path[1:]))

        # If no path, just return this
        return self

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
