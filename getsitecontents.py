"""
Tools to get the files located at a site.

.. Warning::

   Must be used on a machine with xrdfs installed.

:author: Daniel Abercrombie <dabercro@mit.edu>
"""


import re
import time
import datetime
import subprocess
import logging
import threading
import multiprocessing

from . import config
from . import datatypes

LOG = logging.getLogger(__name__)

def get_site(site):
    """
    Get the information for a site, either from a cache or from XRootD.

    :param str site: The site name
    :returns: The site information
    :rtype: DirectoryInfo
    """

    # Get the redirector for a site

    redirector = config.get_redirector(site)

    # Create the filler function for the DirectoryInfo

    error_code_re = re.compile('\[(\!|\d+)\]')
    semaphore = threading.Semaphore(
        config.config_dict().get('MaxThreads') or multiprocessing.cpu_count())

    def ls_directory(path, attempts=0, prev_stdout=''):
        """
        Gets the contents of the previously defined redirector at a given path

        :param str path: The full path starting with ``/store/``.
        :param str attempts: The number of previous attempts.
                             If the total number of attempts is more than the
                             NumberOfAttempts in the config, give back a new dummy file.
                             The young age should lead to the directory being left alone.
        :param str prev_stout: stdout from previous attempt
        :returns: A list of directories and list of file information
        :rtype: tuple
        """

        semaphore.acquire()

        directory_listing = subprocess.Popen(['xrdfs', redirector, 'ls', '-l', path],
                                             stdout=subprocess.PIPE,
                                             stderr=subprocess.PIPE)

        directories = []
        files = []
        # For some reason, we have duplicates. Ignore those with this variable
        inserted = set()

        stdout, stderr = directory_listing.communicate()

        # Parse the stderr
        if stderr:
            LOG.error(stderr.strip())

            # If a number of attempts were already made, give up
            if attempts == config.config_dict().get('NumberOfAttempts', 1):
                LOG.info('Giving up on %s', path)
                stdout += '\n' + prev_stdout

            else:
                # Check against list of "error codes" to retry
                error_code = error_code_re.search(stderr).group(1)
                if error_code in ['!', '3005']:
                    LOG.info('Retrying directory %s', path)
                    time.sleep(1)
                    semaphore.release()
                    return ls_directory(path, attempts + 1,
                                        '\n'.join([stdout, prev_stdout]))

        # Parse the stdout
        for line in stdout.split('\n'):
            # Ignore blank lines
            if not line.strip():
                continue

            # Ignore duplicate lines (which come up a lot)
            elements = line.split()
            name = elements[-1].split('/')[-1]
            if name in inserted:
                continue
            inserted.add(name)

            # Determine if directory or file
            if elements[0][0] == 'd':
                directories.append(name)
            else:
                # For files, append tuple (name, size, mtime)
                files.append(
                    (name, int(elements[-2]),
                     int(time.mktime(
                         datetime.datetime.strptime(
                             '%s %s' % (elements[1], elements[2]),
                             '%Y-%m-%d %H:%M:%S').timetuple()
                             )
                            )
                        )
                        )

        if not directories:
            LOG.info('No more directories in %s, and %i files.', path, len(files))

        semaphore.release()
        return directories, files

    # Create DirectoryInfo for each directory to search
    directories = []

    for directory in config.config_dict().get('DirectoryList', []):
        datatypes.create_dirinfo('/store/', directory,
                                 ls_directory, directories)

    # Merge the DirectoryInfo
    info = datatypes.DirectoryInfo(name='/store', to_merge=directories)

    # Save
    info.save('/tmp/%s_content.pkl' % site)

    # Return the DirectoryInfo
    return info
