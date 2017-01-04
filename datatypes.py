"""
Module contains the datatypes that are used for storage and comparison

:author: Daniel Abercrombie <dabercro@mit.edu>
"""


import os
import hashlib
import pickle


class DirectoryInfo(object):
    """Stores all of the information of a directory"""

    def __init__(self, location='', name='', filler=None, to_merge=None):
        """Create the directory information

        :param str location: The path up to the current directory.
                             In other words, it is the joining of all the
                             parent's names by ``'/'``.
        :param str name: The name of the directory
        :param function filler: A pointer to a function that takes a full
                                path as its only argument and returns a
                                tuple of (directories, files) and
                                the file information is a tuple including
                                all of the desired stuff.
        :param list to_merge: If this is filled, instead of using the location
                              and filler to fill DirectoryInfo, the infos in the
                              list are merged into a master DirectoryInfo.
        """

        if to_merge:
            self.directories = to_merge
            self.files = []

        else:
            full_path = os.path.join(location, name)

            directories, files = filler(full_path)

            self.directories = [DirectoryInfo(full_path, directory, filler) for
                                directory in sorted(directories)]
            self.files = [{
                'name': name,
                'size': size,
                'mtime': mtime,
                'hash': hashlib.sha1(
                    '%s %i %i' % (name, size, mtime)
                    ).hexdigest()
                } for name, size, mtime in sorted(files)]

        hasher = hashlib.sha1()
        ages = []

        for directory in self.directories:
            hasher.update('%s %s' % (directory.name, directory.hash))
            ages.append(directory.oldest)
        for file_info in self.files:
            hasher.update('%s %s' % (file_info['name'], file_info['hash']))
            ages.append(file_info['mtime'])

        self.oldest = min(ages) if ages else 0
        self.name = name
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
