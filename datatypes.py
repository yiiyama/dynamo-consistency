# pylint: disable=bad-option-value, too-many-locals, too-many-branches, too-many-statements, too-complex
#
# Here there be dragons.
# ...
# This will need to be fixed, along with documentation
#

"""
Module contains the datatypes that are used for storage and comparison

:author: Daniel Abercrombie <dabercro@mit.edu>
"""


import os
import time
import hashlib
import pickle
import random
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


def create_dirinfo(location, first_dir, filler, object_params=None):
    """ Create the directory information in it's very own thread

    :param str location: For the listing
    :param str name: For the DirectoryInfo constructor
    :param function filler: For the listing
    :param threads: Either the parameters to pass the creation of filler for each thread
                    or an integer listing the number of threads to use
    :type threads: list or int
    :returns: A DirectoryInfo
    :rtype: DirectoryInfo
    """

    LOG.debug('Called create_dirinfo(%s, %s, %s, %s)',
              location, first_dir, filler, object_params)

    # Determine the number of threads
    if not object_params:
        n_threads = config.config_dict()['MaxThreads'] or multiprocessing.cpu_count()
    else:
        n_threads = len(object_params)

    LOG.debug('Number of threads: %i', n_threads)

    # Initialize queue and connection lists
    out_queue = multiprocessing.Queue()
    in_queues = []

    master_conns = []
    slave_conns = []

    for _ in xrange(n_threads):
        in_queues.append(multiprocessing.Queue())

        con1, con2 = multiprocessing.Pipe()
        master_conns.append(con1)
        slave_conns.append(con2)

    # Put in the first element for the queues
    # They go like, (location inside DirectoryInfo, name of inner object,
    #                list of previous directories, list of previous files,
    #                list of queue numbers that have failed so far)
    random.choice(in_queues).put((os.path.join(location, first_dir), '', [], [], []))

    def run_queue(i_queue):
        """
        Runs over one of the queues
        """

        LOG = logging.getLogger('%s--thread%i' % (__name__, i_queue))


        LOG.debug('Running queue: %i', i_queue)
        running = True

        # Get the queue and connection for this thread
        in_queue = in_queues[i_queue]
        conn = slave_conns[i_queue]

        if object_params:
            params = object_params[i_queue]
            LOG.debug('Params for this object: %s', params)
            thread_object = filler(*params)
            filler_func = thread_object.list
        else:
            filler_func = filler

        while running:
            try:
                location, name, prev_dirs, prev_files, failed_list = in_queue.get(True, 3)
                LOG.debug('Getting directory with (%s, %s, %s)',
                          location, name, failed_list)

                # Call filler
                full_path = os.path.join(location, name)
                LOG.debug('Full path is %s', full_path)
                ok, directories, files = filler_func(full_path)
                LOG.debug('Got from filler: Good? %s, %i directories, %i files',
                          ok, len(directories), len(files))

                # Tell master that a job finished
                conn.send('One_Job')
                conn.send(time.time())

                send_results = True

                # If not okay, retry
                if not ok:

                    directories.extend(prev_dirs)
                    files.extend(prev_files)

                    LOG.debug('Full dirs, and files: %s, %s', directories, files)

                    # Duplicate the failed list and add this thread
                    track_failed = list(failed_list)
                    track_failed.append(i_queue)
                    LOG.debug('Creating retry list, excluding %s', track_failed)

                    LOG.debug('All available queues: %s', in_queues)
                    retry_candidates = [queue for queue in range(n_threads) \
                                            if queue not in track_failed]
                    LOG.debug('Retry candidates created: %s', retry_candidates)

                    # If there are threads where we can retry this, put the input there
                    if retry_candidates:
                        send_results = False
                        retry_queue = random.choice(retry_candidates)
                        LOG.debug('Will retry in queue %s', retry_queue)
                        in_queues[retry_queue].put((location, name, directories, files, track_failed))
                        LOG.debug('Put in queue %s', retry_queue)
                    # Otherwise, give up and output
                    else:
                        LOG.debug('Giving up directory %s', full_path)
                        files.append(('_unlisted_', 0, 0))

                # On success, we do the normal input and output queues
                if send_results:
                    out_queue.put((name, directories, files, len(failed_list)))

                    # Add each directory into some input queue
                    for directory, _ in directories:
                        joined_name = os.path.join(name, directory)
                        sizes = [queue.qsize() for queue in in_queues]
                        in_queues[sizes.index(min(sizes))].\
                            put((location, joined_name, [], [], []))


                LOG.debug('Finished one job with (%s, %s)', location, name)
            except Empty:
                # Report empty
                LOG.info('Worker finished input queue')
                conn.send('All_Job')

                # Check for main process
                message = conn.recv()                
                LOG.debug('Message from master: %s', message)
                # If permission, close
                if message == 'Close':
                    conn.close()
                    running = False

    # Spawn processes
    processes = []

    for i_queue in range(n_threads):
        process = multiprocessing.Process(target=run_queue, args=(i_queue,))
        process.start()
        processes.append(process)

    # Build the DirectoryInfo
    building = True
    dir_info = DirectoryInfo(first_dir)

    while building:
        try:
            # Get the info from the queue
            name, directories, files, _ = out_queue.get(True, 1)

            # Create the nodes and files
            built = dir_info.get_node(name)
            built.add_files(files)

            # Set correct node mtime for directories
            for directory, mtime in directories:
                built.get_node(directory).mtime = mtime

        except Empty:
            LOG.debug('Empty queue for building.')
            LOG.info('Number of files so far built: %i', dir_info.get_num_files())

            # Ends only if all threads are done at the beginning of this check
            threads_done = 0
            for conn in master_conns:
                LOG.debug('Waiting for thread %i', master_conns.index(conn))
                message = conn.recv()
                LOG.debug('Recieved message %s', message)
                # Count the number of threads saying their finished at the beginning
                if message == 'All_Job':
                    threads_done += 1
                    LOG.debug('Threads saying done: %i', threads_done)
                    # Send back to work, just in case not all threads are done
                    conn.send('Work')
                elif message == 'One_Job':
                    LOG.debug('Found one job, about to cycle')
                    now = time.time()
                    cycle = True
                    while cycle:
                        # Cycle through timestamps so that we do not have a backlog
                        timestamp = conn.recv()
                        if now - timestamp < 0.0:
                            LOG.debug('Found new message, breaking.')
                            cycle = False
                        else:
                            mess = conn.recv()
                            if mess == 'All_Job':
                                LOG.debug('Found end to pipe.')
                                conn.send('Work')
                                cycle = False
                else:
                    LOG.error('Weird message from pipe')
 
            if threads_done == n_threads:
                LOG.debug('Done building')
                building = False

    LOG.debug('Closing all connections')
    # Tell connections to close
    for conn in master_conns:
        conn.send('Close')
        conn.close()

    LOG.debug('Waiting for processes')
    # Wait for processes to join
    for proc in processes:
        proc.join()

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

        else:
            self.directories = directories or []

        self.timestamp = time.time()
        self.name = name
        self.hash = None
        self.files = []
        self.add_files(files)
        self.mtime = None

        self.can_compare = False

    def add_files(self, files, num_fails=0):
        """
        Set the files for this DirectoryInfo node

        :param list files: The tuples of file information
        """
        self.files.extend([{
            'name': name,
            'size': size,
            'mtime': mtime,
            'hash': hashlib.sha1(
                '%s %i' % (name, size)
                ).hexdigest(),
            'can_compare': bool(mtime + IGNORE_AGE * 24 * 3600 < self.timestamp and
                                name != '_unlisted_')
            } for name, size, mtime in sorted(files or [])])

    def add_file_list(self, file_infos):
        """
        Add a list of tuples containing file_name, file_size to the node

        :param list file_infos: The list of files (full path, size in bytes)
        """

        files = []
        directory = ''

        for name, size in file_infos:
            if directory and \
                    name.startswith(os.path.join(self.name, directory)):
                files.append((os.path.basename(name), size, 0))
            else:
                self.get_node(directory).add_files(files)
                directory = os.path.dirname(name[len(self.name):].lstrip('/'))
                files = [(os.path.basename(name), size, 0)]

        self.get_node(directory).add_files(files)


    def setup_hash(self):
        """
        Set the hashes and times for this DirectoryInfo
        """

        hasher = hashlib.sha1()

        self.directories.sort(key=lambda x: x.name)
        self.files.sort(key=lambda x: x['name'])

        hasher.update(self.name)

        for directory in self.directories:
            directory.setup_hash()
            self.can_compare = self.can_compare or directory.can_compare

            # Ignore newer directories
            if directory.can_compare:
                hasher.update('%s %s' % (directory.name, directory.hash))

        LOG.debug('Making hash for directory named %s', self.name)

        for file_info in self.files:
            if file_info['can_compare']:
                self.can_compare = True
                hasher.update('%s %s' % (file_info['name'], file_info['hash']))

            LOG.debug('File included: %s size: %i can compare: %i',
                      file_info['name'], file_info['size'], int(file_info['can_compare']))

        if not (self.directories or self.files) and self.mtime and \
                self.mtime + IGNORE_AGE * 24 * 3600 < self.timestamp:
            self.can_compare = True

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
        print self.displays(path)

    def displays(self, path=''):
        """
        Get the string to print out the contents of this DirectoryInfo

        :param str path: The full path to this DirectoryInfo instance
        :returns: The display string
        :rtype: str
        """

        if not path:
            path = self.name

        output = 'compare: %i my hash: %s path: %s' % (int(self.can_compare), self.hash, path)
        for file_info in self.files:
            output += ('\nmtime: %i size: %i my hash:%s name: %s' %
                       (file_info['mtime'], file_info['size'],
                        file_info['hash'], file_info['name']))

        for directory in self.directories:
            output += '\n' + directory.displays(os.path.join(path, directory.name))

        return output

    def get_node(self, path, make_new=True):
        """ Get the node that corresponds to the path given

        :param str path: Path to the desired node from current node.
                         If the path does not exist yet, empty nodes will be created.
        :param str make_new: Bool to create new node if none exists at path or not
        :returns: A node with the proper path, unless make_new is False and the node doesn't exist
        :rtype: DirectoryInfo or None
        """

        # If any path left
        if path:
            split_path = path.split('/')
            return_name = '/'.join(split_path[1:])

            # Search for if directory exists
            for directory in self.directories:
                if split_path[0] == directory.name:
                    return directory.get_node(return_name, make_new)

            # If not, make a new directory, or None
            if make_new:
                new_dir = DirectoryInfo(split_path[0])
                self.directories.append(new_dir)
                return new_dir.get_node(return_name, make_new)
            else:
                return None

        # If no path, just return this
        return self

    def get_num_files(self):
        """ Report the total number of files stored.

        :returns: The number of files in the directory tree structure
        :rtype: int
        """

        num_files = len(self.files)
        for directory in self.directories:
            num_files += directory.get_num_files()

        return num_files

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

    def compare(self, other, path=''):
        """
        Does one way comparison with a different tree

        :param DirectoryInfo other: The directory tree to compare this one to
        :param str path: Is the path to get to this location so far
        :returns: Tuple of list of files and directories that are present and not in the other tree
                  and the size of the files that corresponds to
        :rtype: list, list, int
        """

        extra_files = []
        extra_dirs = []
        extra_size = 0

        here = os.path.join(path, self.name)

        if other:
            if self.hash != other.hash:
                for directory in self.directories:
                    if not directory.can_compare:
                        continue

                    new_other = other.get_node(directory.name, False)
                    more_files, more_dirs, more_size = directory.compare(new_other, here)
                    extra_size += more_size
                    extra_files.extend(more_files)
                    if new_other:
                        extra_dirs.extend(more_dirs)
                    elif '_unlisted_' not in [fi['name'] for fi in other.files]:
                        extra_dirs.append(os.path.join(here, directory.name))

                for file_info in self.files:
                    if not file_info['can_compare']:
                        continue

                    found = False
                    for to_match in other.files:
                        if file_info['hash'] == to_match['hash'] or \
                                to_match['name'] == '_unlisted_':
                            found = True
                            break

                    if not found:
                        extra_files.append(os.path.join(path, self.name, file_info['name']))
        else:
            if self.files:
                for file_info in self.files:
                    extra_files.append(os.path.join(path, self.name, file_info['name']))
                    extra_size += file_info['size']

            for directory in self.directories:
                more_files, _, more_size = directory.compare(None, here)
                extra_size += more_size
                extra_files.extend(more_files)

        return extra_files, extra_dirs, extra_size

    def listdir(self, *args, **kwargs):
        """
        Get the list of directory names within a DirectoryInfo.
        Adding an argument will display the contents of the matching directory
        that is displayed when there is one less argument.

        :param args: Is a list of indices to list the subdirectories
        :param kwargs: Supports 'printing' which is set to a bool. Defaults as True.
        :returns: The DirectoryInfo that is listed
        :rtype: DirectoryInfo
        """

        printing = kwargs.get('printing', True)

        if printing:

            print '\nDirectories:'

            if self.directories:
                width = max([len(di.name) for di in self.directories]) + 2
            else:
                width = 0

            for index, directory in enumerate(self.directories):
                print '%i: %-{0}s %s  Num Files: %i'.format(width) % \
                    (index, directory.name, directory.hash, directory.get_num_files())

        if args:
            return self.directories[args[0]].listdir(*args[1:], printing=printing)

        elif printing:
            print 'Files:'
            for file_info in self.files:
                print file_info

        return self

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


def compare(inventory, listing, output_base):
    """
    Compare two different trees and output the differences into an ASCII file

    :param DirectoryInfo inventory: The tree of files that should be at a site
    :param DirectoryInfo listing: The tree of files that are listed remotely
    :param str output_base: The names of the ASCII files to place the reports
                            are generated from this variable.
    """

    missing, _, _ = inventory.compare(listing)
    orphan, _, _ = listing.compare(inventory)

    with open('%s_missing.txt' % output_base, 'w') as missing_file:
        for line in missing:
            missing_file.write(line + '\n')

    with open('%s_orphan.txt' % output_base, 'w') as orphan_file:
        for line in orphan:
            orphan_file.write(line + '\n')
