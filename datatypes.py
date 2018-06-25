# pylint: disable=too-many-locals, too-many-branches, too-many-statements, too-complex
#
# Here there be dragons
#

"""
Module defines the datatypes that are used for storage and comparison.
There is also a powerful create_dirinfo function that takes a filler function
or object and uses the multiprocessing module to recursively list directories
in parallel.

:author: Daniel Abercrombie <dabercro@mit.edu>
"""


import os
import time
import hashlib
import cPickle
import logging
import multiprocessing

from Queue import Empty

from . import config

LOG = logging.getLogger(__name__)
IGNORE_AGE = float(config.config_dict()['IgnoreAge'])
"""
The maximum age, in days, of files and directories to ignore in this check.
This variable should be reset once in a while by deamons that run while an
operator might be adjusting the configuration.
"""


def create_dirinfo(location, first_dir, filler,
                   object_params=None, callback=None):
    """ Create the directory information

    :param str location: This is the beginning of the path where we will find ``first_dir``.
                         For example, to find the first directory ``mc``, we also have to
                         say where it is. In most cases, using LFNs, location would be
                         ``/store/`` (where ``mc`` is inside).
                         This is a path.
    :param str first_dir: The name of the first directory that is inside the path of ``location``.
                          This should not be a path,
                          but the name of the directory to list recursively.
    :param filler: This is either a function that lists the directory contents given just a path
                   of ``os.path.join(location, first_dir)``, or it is a constructor that
                   does the same thing with a member function called ``list``.
                   If ``filler`` is an object constructor, the parameters for the object
                   creation must be passed through the parameter ``object_params``.
                   Both listings must return the following tuple:

                     - A bool saying whether the listing was successful or not
                     - A list of tuples of sub-directories and their mod times
                     - A list of tuples files inside, their size, and their mode times

    :type filler: function or constructor
    :param list object_params: This only needs to be set when filler is an object constructor.
                               Each element in the list is a tuple of arguments to pass
                               to the constructor.
    :param function callback: A function that is called every time master thread has finished
                              checking the child threads.
                              This can happen very many times at large sites.
                              The function is called with the main DirectoryTree as its argument
    :returns: A :py:class:`DirectoryInfo` object containing everything the directory listings from
              ``os.path.join(location, first_dir)`` with name ``first_dir``.
    :rtype: DirectoryInfo
    """

    LOG.debug('Called create_dirinfo(%s, %s, %s, %s)',
              location, first_dir, filler, object_params)

    # Determine the number of threads
    if object_params is not None:
        n_threads = len(object_params)
    else:
        n_threads = config.config_dict()['NumThreads'] or multiprocessing.cpu_count()

    # First directory is location + first_dir
    starting_dir = os.path.join(location, first_dir)
    LOG.info('Listing directory %s with %i threads', starting_dir, n_threads)

    # Initialize queue and connection lists
    out_queue = multiprocessing.Queue()
    in_queue = multiprocessing.Queue()

    master_conns = []
    slave_conns = []
    send_to_master = []

    for _ in xrange(n_threads):
        con1, con2 = multiprocessing.Pipe()
        master_conns.append(con1)
        slave_conns.append(con2)
        send_to_master.append(multiprocessing.Queue())

    # Put in the first element for the queue
    # They go like, (full path of the next listing to do,
    #                name of sub-node to place the listing (blank for first level),
    #                list of previous directories, list of previous files (for retries),
    #                list of queue numbers that have failed so far)
    in_queue.put((starting_dir, '', [], [], []))

    def run_queue(i_queue):
        """
        Runs over one of the queues.
        When the queue is finished, it checks back with the master for permission to stop.

        :param int i_queue: The number of the thread
        """

        thread_log = logging.getLogger('%s--thread%i' % (__name__, i_queue))

        thread_log.debug('Running queue: %i', i_queue)
        running = True

        # Get the queue and connection for this thread
        conn = slave_conns[i_queue]

        if object_params:
            # Create the object with the parameters here
            params = object_params[i_queue]
            thread_log.debug('Params for this object: %s', params)
            thread_object = filler(*params)
            filler_func = thread_object.list
        else:
            # Otherwise, use the filler function directly passed
            filler_func = filler

        while running:
            try:
                location, name, prev_dirs, prev_files, failed_list = in_queue.get(True, 3)
                if i_queue in failed_list:
                    thread_log.warning('Got previously failed call, putting back and sleeping')
                    in_queue.put((location, name, prev_dirs, prev_files, failed_list))
                    time.sleep(10)

                thread_log.debug('Getting directory with (%s, %s, %s)',
                                 location, name, failed_list)

                # Call filler
                full_path = os.path.join(location, name)
                thread_log.debug('Full path is %s', full_path)
                okay, directories, files = filler_func(full_path)
                thread_log.debug('Got from filler: Good? %s, %i directories, %i files',
                                 okay, len(directories), len(files))

                # If not okay, add _unlisted_ flag
                if not okay:

                    directories = list(set(directories + prev_dirs))
                    files = list(set(files + prev_files))

                    thread_log.debug('Full dirs, and files: %s, %s', directories, files)

                    thread_log.error('Giving up directory %s', full_path)
                    # _unlisted_ is used as a flag to tell our comparer something went wrong
                    files.append(('_unlisted_', 0, 0))

                # Send results to master queue
                out_queue.put((name, directories, files, len(failed_list)))

                # Add each directory into some input queue
                for directory, _ in directories:
                    joined_name = os.path.join(name, directory)
                    in_queue.put((location, joined_name, [], [], []))

                # Tell master that a job finished,
                # so it can build the final object
                send_to_master[i_queue].put(('O', time.time()))

                thread_log.debug('Finished one job with (%s, %s)', location, name)
            except Empty:
                # Report empty
                thread_log.debug('Worker finished input queue')
                send_to_master[i_queue].put(('A', 0))
                #conn.send('All_Job')

                # Check for main process
                message = conn.recv()
                thread_log.debug('Message from master: %s', message)
                # If permission, close
                if message == 'Close':
                    conn.close()
                    running = False
                else:
                    thread_log.debug('Worker going back to check queue')

    # Spawn processes to run on this run_queue function
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
            # When empty, check on the status of the workers
            LOG.debug('Empty queue for building.')
            LOG.info('Number of files so far built: %8i  nodes: %8i',
                     dir_info.get_num_files(), dir_info.count_nodes())

            # Process the dir_info with some callback
            if callback:
                callback(dir_info)

            # Ends only if all threads are done at the beginning of this check
            threads_done = 0
            for conn in master_conns:
                LOG.debug('Waiting for thread %i', master_conns.index(conn))
                message, timestamp = send_to_master[master_conns.index(conn)].get(True)

                LOG.debug('Recieved message %s', message)
                # Count the number of threads saying their finished at the beginning
                if message == 'A':
                    threads_done += 1
                    LOG.info('Threads saying done: %i', threads_done)
                    # Send back to work, just in case not all threads are done
                    conn.send('Work')
                elif message == 'O':
                    # This thread wasn't finished at the beginning, so threads_done
                    # will not reach n_threads if the master reaches this point in the code
                    LOG.debug('Found one job, about to cycle')
                    now = time.time()
                    cycle = True
                    while cycle:
                        # Cycle through timestamps so that we do not have a backlog
                        try:
                            message, timestamp = \
                                send_to_master[master_conns.index(conn)].get(True, 1)
                            if message == 'A':
                                LOG.debug('Found end to pipe.')
                                conn.send('Work')
                                cycle = False
                            elif timestamp > now:
                                cycle = False
                        except Empty:
                            cycle = False

                else:
                    LOG.error('Weird message from pipe')

            # Check if all the threads were finished
            if threads_done == n_threads:
                LOG.debug('Done building')
                # Break out of loop of checking
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

    return dir_info


class NotEmpty(Exception):
    """
    An exception for throwing when a non-empty directory is deleted
    from a :py:class:`DirectoryInfo`
    """
    pass


class BadPath(Exception):
    """
    An exception for throwing when the path doesn't make sense for various methods
    of a :py:class:`DirectoryInfo`
    """
    pass


class DirectoryInfo(object):
    """
    Stores all of the information of the contents of a directory

    :param str name: The name of the directory
    :param list directories: If this is set, the infos in the
                             list are merged into a master :py:class:`DirectoryInfo`.
    :param list files: List of tuples containing information about files
                       in the directory.
    """

    __slots__ = ('directories', 'timestamp', 'name', 'hash', 'files', 'mtime', 'can_compare')
    def __init__(self, name='', directories=None, files=None):
        self.directories = directories or []
        self.timestamp = time.time()
        self.name = name
        self.hash = None
        # Is only None until filled for the first time.
        # If still None for some reason during comparison, errors will be thrown
        self.files = None
        self.mtime = None

        self.can_compare = False

        if directories is not None or files is not None:
            self.add_files(files)


    def get_files(self, min_age=0, path=''):
        """
        Get the list of files that are older than some age

        :param int min_age: The minimum age, in seconds, of files to list
        :param str path: The path to this file. Used for recursive calls
        :returns: List of full file paths
        :rtype: list
        """

        output = []
        for fil in self.files:
            # Only list old files
            if (self.timestamp - fil['mtime']) > min_age and fil['name'] != '_unlisted_':
                output.append(os.path.join(path, self.name, fil['name']))

        for directory in self.directories:
            output.extend(directory.get_files(min_age, os.path.join(path, self.name)))

        return output


    def add_files(self, files):
        """
        Set the files for this :py:class:`DirectoryInfo` node

        :param list files: The tuples of file information.
                           Each element consists of file name, size, and mod time.
        :returns: self for chaining calls
        :rtype: :py:class:`DirectoryInfo`
        """

        # This is where we know that the directory has been properly filled
        if self.files is None:
            self.files = []

        # Get the list of new files
        existing_names = set(fi['name'] for fi in self.files)
        sorted_files = [fi for fi in sorted(files or []) \
                            if fi[0] not in existing_names]

        for file_info in sorted_files:
            name, size, mtime = file_info[:3]

            if len(file_info) > 3:
                block = file_info[3]
            else:
                block = ''

            self.files.append({
                'name': name,
                'size': long(size),
                'mtime': mtime,
                'block': block,
                'hash': hashlib.sha1(
                    '%s %i' % (name, size)    # We are not comparing mtime for now
                    ).hexdigest(),
                'can_compare': bool(mtime + IGNORE_AGE * 24 * 3600 < self.timestamp and
                                    name != '_unlisted_')
                })

        self.files.sort(key=lambda x: x['name'])

        return self

    def add_file_list(self, file_infos):
        """
        Add a list of tuples containing file_name, file_size to the node.
        This is most useful when you get a list of files from some other source
        and want to easily convert that list into a :py:func:`DirectoryInfo`

        :param list file_infos: The list of files (full path, size in bytes[, timestamp])
        """

        files = []
        directory = ''

        for file_info in file_infos:

            name, size = file_info[:2]
            if len(file_info) > 2:
                timestamp = file_info[2]
            else:
                timestamp = 0

            new_dir = os.path.dirname(name[len(self.name):].lstrip('/'))

            if directory == new_dir:
                # If in the old directory, append to the list of files
                files.append((os.path.basename(name), size, timestamp))
            else:
                # When changing directories, append the files gathered in the last directory
                self.get_node(directory).add_files(files)
                # Get the new directory name
                directory = new_dir
                # Reset the files list
                files = [(os.path.basename(name), size, timestamp)]

        # Add data from the last directory
        self.get_node(directory).add_files(files)

    def setup_hash(self):
        """
        Set the hashes for this :py:class:`DirectoryInfo`
        """

        if self.files is None:
            return

        hasher = hashlib.sha1()

        # Sort the sub-directories and files
        self.directories.sort(key=lambda x: x.name)
        self.files.sort(key=lambda x: x['name'])

        hasher.update(self.name)

        for directory in self.directories:
            # Recursively make the hash for each subdirectory first
            directory.setup_hash()
            # Can compare if a subdirectory asks for it
            self.can_compare = self.can_compare or directory.can_compare

            # Ignore newer directories or any others that don't want to be compared
            if directory.can_compare:
                hasher.update('%s %s' % (directory.name, directory.hash))

        for file_info in self.files:
            if file_info['can_compare']:
                # Add files that can be compared, and set self to be compared
                self.can_compare = True
                hasher.update('%s %s' % (file_info['name'], file_info['hash']))

        # Add empty directories that are not too new to comparison
        if not (self.directories or self.files) and self.mtime and \
                self.mtime + IGNORE_AGE * 24 * 3600 < self.timestamp:
            self.can_compare = True

        # Calculate hash
        self.hash = hasher.hexdigest()


    def save(self, file_name):
        """
        Save this :py:class:`DirectoryInfo` in a file.

        :param str file_name: is the location to save the file
        """

        with open(file_name, 'w') as outfile:
            cPickle.dump(self, outfile, protocol=cPickle.HIGHEST_PROTOCOL)

    def display(self, path=''):
        """
        Print out the contents of this :py:class:`DirectoryInfo`

        :param str path: The full path to this :py:class:`DirectoryInfo` instance
        """
        print self.displays(path)

    def displays(self, path=''):
        """
        Get the string to print out the contents of this :py:class:`DirectoryInfo`.

        :param str path: The full path to this :py:class:`DirectoryInfo` instance
        :returns: The display string
        :rtype: str
        """
        # This is in a separate function for unit test assertion errors, which likes strings

        if not path:
            path = self.name

        output = 'compare: %i mtime: %s my hash: %s path: %s' % \
            (int(self.can_compare), str(self.mtime), self.hash, path)

        for file_info in self.files:
            output += ('\nmtime: %i size: %i my hash:%s name: %s' %
                       (file_info['mtime'], file_info['size'],
                        file_info['hash'], file_info['name']))

        for directory in self.directories:
            # Recursively get displays for sub-directories
            output += '\n' + directory.displays(os.path.join(path, directory.name))

        return output

    def get_node(self, path, make_new=True):
        """ Get the node that corresponds to the path given.
        If the node does not exist yet, and ``make_new`` is True, the node is created.

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
                # If we're making a new directory, then this should have non-None self.files
                if self.files is None:
                    self.files = []

                new_dir = DirectoryInfo(split_path[0])
                self.directories.append(new_dir)
                return new_dir.get_node(return_name, make_new)

            return None

        # If no path, just return self
        return self

    def get_directory_size(self):
        """ Report the total size used by this directory and its subdirectories.

        :returns: Size of files in directory, in bytes
        :rtype: int
        """

        return sum([di.get_directory_size() for di in self.directories],
                   sum([fi['size'] for fi in self.files]))

    def get_unlisted(self, path=''):
        """
        :param str path: Path to prepend to the name, used in recursive calls
        :returns: List of directories that were unlisted
        :rtype: list
        """
        here = os.path.join(path, self.name)
        output = [name for d in self.directories for name in d.get_unlisted(here)]
        if '_unlisted_' in [f['name'] for f in self.files]:
            output.append(here)

        return output

    def get_num_files(self, unlisted=False, place_new=False):
        """ Report the total number of files stored.

        :param bool unlisted: If true, return number of unlisted directories,
                              Otherwise return only successfully listed files
        :param bool place_new: If true, pretend there's one more file inside
                               any new directory or if files is None.
                               This prevents listing of empty directories to include
                               directories that should not actually be deleted.
        :returns: The number of files in the directory tree structure
        :rtype: int
        """

        if self.files is None:
            return int(place_new)

        num_files = len([fi for fi in self.files \
                             if (fi['name'] == '_unlisted_') == unlisted])
        for directory in self.directories:
            num_files += directory.get_num_files(unlisted, place_new)

        if place_new and (not self.can_compare or self.mtime is None):
            num_files += 1

        return num_files

    def _grab_first(self, levels=100):
        """ Used for debugging.
        Grabs the subdirectories by the first in the list.

        :param int levels: is the number of levels of directories to bypass
        :returns: The proper :py:class:`DirectoryInfo` level
        :rtype: DirectoryInfo
        """

        output = self

        for _ in xrange(levels):
            if output.directories:
                output = output.directories[0]
            else:
                break

        return output

    def compare(self, other, path='', check=None):
        """ Does one way comparison with a different tree

        :param DirectoryInfo other: The directory tree to compare this one to
        :param str path: Is the path to get to this location so far
        :param check: An optional function that double checks a file name.
                      If the checking function returns ``True`` for a file name,
                      the file will not be included in the output.
        :type check: function
        :returns: Tuple of list of files and directories that are present and not in the other tree
                  and the size of the files that corresponds to
        :rtype: list, list, long
        """

        extra_files = []
        extra_dirs = []
        extra_size = long(0)

        if '_unlisted_' in [fi['name'] for fi in self.files]:
            return extra_files, extra_dirs, extra_size

        here = os.path.join(path, self.name)

        if other:
            # If there is a match in the hash, then the nodes are effectively identical
            # Otherwise, do these recursive comparisons

            logging.debug('Hashes: %s -- %s, can compare: %i -- %i',
                          self.hash, other.hash, self.can_compare, other.can_compare)

            if self.hash != other.hash and other.can_compare:
                for directory in self.directories:
                    # Ignore not comparable directories (usually new ones)
                    if not directory.can_compare:
                        continue

                    # Recursive check of extra files and directories here
                    new_other = other.get_node(directory.name, False)
                    more_files, more_dirs, more_size = directory.compare(new_other, here, check)
                    extra_size += more_size
                    extra_files.extend(more_files)
                    if new_other:
                        extra_dirs.extend(more_dirs)
                    elif '_unlisted_' not in [fi['name'] for fi in other.files]:
                        # If the subdirectory does not exist, and '_unlisted_' not thrown
                        # mark that whole directory as being extra.
                        # At the moment this is redundant with all the files,
                        # but gives a good place to prune file system directories
                        # after files have been deleted
                        extra_dirs.append(os.path.join(here, directory.name))

                for file_info in self.files:
                    if not file_info['can_compare']:
                        continue

                    # See if each file exists and has the correct hash
                    # Say all files are fine in a directory that is even partially '_unlisted_'
                    found = False
                    for to_match in other.files:
                        if file_info['hash'] == to_match['hash'] or \
                                to_match['name'] == '_unlisted_':
                            found = True
                            break

                    full_name = os.path.join(path, self.name, file_info['name'])

                    if not found and (check is None or not check(full_name)):
                        extra_size += file_info['size']
                        extra_files.append(full_name)
        else:
            # If no other node to compare, all files are extra (not in the other tree)

            LOG.debug('Nothing to compare, files: %s', self.files)
            LOG.debug('Nothing to compare, directories: %s',
                      [(di.name, di.can_compare) for di in self.directories])

            for file_info in [fi for fi in self.files if fi['can_compare']]:
                full_name = os.path.join(path, self.name, file_info['name'])

                if check is None or not check(full_name):
                    extra_files.append(os.path.join(path, self.name, file_info['name']))
                    extra_size += file_info['size']

            # All directories are extra too
            for directory in [di for di in self.directories if di.can_compare]:
                more_files, _, more_size = directory.compare(None, here, check)
                extra_size += more_size
                extra_files.extend(more_files)

        return extra_files, extra_dirs, extra_size

    def count_nodes(self, empty=False):
        """
        :param bool empty: If True, only return the number of empty nodes
        :returns: The total number of nodes in this Directory Info. This corresponds
                  to approximately the number of listing requests required to build the data.
        :rtype: int
        """

        count_this = 0 if self.files is None or (empty and self.get_num_files() != 0) else 1
        return sum([directory.count_nodes(empty) for directory in self.directories], count_this)

    def empty_nodes_set(self):
        """
        This function recursively builds the entire list of empty  directories that can be deleted

        :returns: The set of empty directories to delete
        :rtype: set
        """

        output = set()

        if not self.can_compare or \
                (self.mtime is not None and self.mtime + IGNORE_AGE * 24 * 3600 > self.timestamp):
            return output

        # Count direct subdirectories that are removed
        count_sub = 0

        for directory in self.directories:
            # Add all the elements from the other set
            for sub in directory.empty_nodes_set():
                if '/' not in sub:
                    count_sub += 1
                output.add(os.path.join(self.name, sub))

        if not (self.get_num_files(place_new=True) or self.mtime is None) and \
                count_sub == len(self.directories):
            output.add(self.name)

        return output

    def empty_nodes_list(self):
        """
        This function should be used to get the nodes to delete in
        the proper order for non-recursive deletion

        :returns: The list of empty directories to delete in the order to delete
        :rtype: list
        """
        # Don't want to recursively sort, so we send this to a helpful set function
        return sorted(self.empty_nodes_set(), reverse=True)

    def listdir(self, *args, **kwargs):
        """
        Get the list of directory names within a :py:class:`DirectoryInfo`.
        Adding an argument will display the contents of the next directory.
        For example, if ``dir.listdir()`` returns::

            0: data
            1: mc

        ``dir.listdir(1)`` then lists the contents of ``mc`` and ``dir.listdir(1, 0)``
        lists the contents of the first subdirectory in ``mc``.

        :param args: Is a list of indices to list the subdirectories
        :param kwargs: Supports 'printing' which is set to a bool. Defaults as True.
        :returns: The :py:class:`DirectoryInfo` that is being listed
        :rtype: DirectoryInfo
        """

        printing = kwargs.get('printing', True)

        # Print the contents of a directory picked next, and return that DirectoryInfo
        if args:
            return self.directories[args[0]].listdir(*args[1:], printing=printing)

        # If we got to the last directory of the args, print the files contained
        elif printing:
            print '\nDirectories:'

            # Get the formatting width for printing the directory names
            if self.directories:
                width = max([len(di.name) for di in self.directories]) + 2
            else:
                width = 0

            # Print information for each directory
            for index, directory in enumerate(self.directories):
                print '%3i: %-{0}s Hash: %s  Num Files: %7i  Dirs Unlisted: %7i'.format(width) % \
                    (index, directory.name, directory.hash,
                     directory.get_num_files(), directory.get_num_files(True))

            if self.files:
                print 'Files:'

            for file_info in self.files:
                print file_info

        return self

    def get_file(self, file_name):
        """
        Get the file dictionary based off the name.

        :param str file_name: The LFN of the file
        :returns: Dictionary of file information
        :rtype: dict
        :raises BadPath: if the file_name does not start with ``self.name``
        """

        if not file_name.startswith(self.name):
            raise BadPath('self.name is %s, file_name is %s' % (self.name, file_name))

        exploded_name = file_name[len(self.name) + 1:].split('/')
        desired_name = exploded_name[-1]
        node = self.get_node('/'.join(exploded_name[:-1]))

        for file_info in node.files:

            if file_info['name'] == desired_name:
                return file_info

        return None

    def remove_node(self, path_name):
        """
        Remove an empty node from the DirectoryInfo

        :param str path_name: The path to the node, including the ``self.name`` at the beginning
        :returns: self for chaining
        :rtype: :py:class:`DirectoryInfo`
        :raises NotEmpty: if the directory is not empty or ``self.files`` is None
        :raises BadPath: if the path_name does not start with the ``self.name``
        """

        LOG.debug('Would like to remove %s', path_name)

        if not path_name.startswith(self.name):
            raise BadPath('self.name is %s, path_name is %s' % (self.name, path_name))

        exploded_name = path_name[len(self.name) + 1:].split('/')
        parent = self.get_node('/'.join(exploded_name[:-1]))

        # If the directory doesn't exist, we'll get some TypeError things
        node = parent.get_node(exploded_name[-1], make_new=False)

        if node.files:
            raise NotEmpty('This directory has files %s' % node.files)
        if node.directories:
            raise NotEmpty('This directory contains subdirectories %s' %
                           [d.name for d in node.directories])
        if node.files is None:
            raise NotEmpty('The files list is still None')
        if node.mtime + IGNORE_AGE * 24 * 3600 > node.timestamp:
            raise NotEmpty('This directory is not old enough?')

        parent.directories.remove(node)
        return self


def get_info(file_name):
    """
    Get the :py:class:`DirectoryInfo` from a file.

    :param str file_name: is the location of the saved information
    :returns: Saved info
    :rtype: DirectoryInfo
    """

    infile = open(file_name, 'r')
    output = cPickle.load(infile)
    infile.close()

    return output


def compare(inventory, listing, output_base=None, orphan_check=None, missing_check=None):
    """
    Compare two different trees and output the differences into an ASCII file

    :param DirectoryInfo inventory: The tree of files that should be at a site
    :param DirectoryInfo listing: The tree of files that are listed remotely
    :param str output_base: The names of the ASCII files to place the reports
                            are generated from this variable.
    :param function orphan_check: A function that double checks each expected orphan.
                                  The function takes as an input, an LFN.
                                  If the function returns true, the LFN will not be
                                  listed as an orphan.
    :param function missing_check: A function checks each expected missing file
                                   The function takes as an input, an LFN.
                                   If the function returns true, the LFN will not be
                                   listed as missing.
    :returns: The two lists, missing and orphan files
    :rtype: tuple
    """

    LOG.info('About to perform comparison. Results will be in files starting with %s',
             output_base)
    LOG.debug('Double checking missing with %s', missing_check)
    missing, _, m_size = inventory.compare(listing, check=missing_check)
    LOG.info('There are %i missing files', len(missing))
    LOG.info('Size: %i', m_size)
    LOG.debug('Double checking orphans with %s', orphan_check)
    orphan, _, o_size = listing.compare(inventory, check=orphan_check)
    LOG.info('There are %i orphan files', len(orphan))
    LOG.info('Size: %i', o_size)

    if output_base:
        with open('%s_missing.txt' % output_base, 'w') as missing_file:
            for line in missing:
                missing_file.write(line + '\n')

        with open('%s_orphan.txt' % output_base, 'w') as orphan_file:
            for line in orphan:
                orphan_file.write(line + '\n')

    return missing, m_size, orphan, o_size
