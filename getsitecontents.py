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

from . import config
from . import datatypes

LOG = logging.getLogger(__name__)

def get_site_tree(site):
    """
    Get the information for a site, either from a cache or from XRootD.

    :param str site: The site name
    :returns: The site information
    :rtype: DirectoryInfo
    """

    # Get the redirector for a site

    redirector = config.get_redirector(site)

    # Create the filler function for the DirectoryInfo

    error_code_re = re.compile(r'\[(\!|\d+|FATAL)\]')

    def ls_directory(path, attempts=0, prev_stdout=''):
        """
        Gets the contents of the previously defined redirector at a given path

        :param str path: The full path starting with ``/store/``.
        :param int attempts: The number of previous attempts.
                             If the total number of attempts is more than the
                             NumberOfRetries in the config, give back a new dummy file.
                             The young age should lead to the directory being left alone.
        :param str prev_stdout: stdout from previous attempt
        :returns: A list of directories and list of file information
        :rtype: tuple
        """

        LOG.debug('Calling ls_directory with: path=%s, attempts=%i, prev_stdout=%i',
                  path, attempts, int(bool(prev_stdout.strip())))

        if attempts:
            LOG.info('Retrying directory %s', path)
            LOG.info('Sleeping for %i seconds before retry.', attempts)
            time.sleep(attempts)

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
            LOG.warning(stderr.strip())
            stdout += '\n' + prev_stdout

            # If full number of attempts haven't been made, tray again
            if attempts != config.config_dict().get('NumberOfRetries', 0):
                # Check against list of "error codes" to retry
                error_code = error_code_re.search(stderr).group(1)

                # I should actually raise an exception here
                if error_code in ['!', '3005']:
                    return ('_retry_', (path, attempts + 1, stdout))

            else:
                LOG.error('Giving up on listing directory %s', path)
                files.append(('_unlisted_', 0, 0))

        # Parse the stdout, skipping blank lines

        LOG.debug('STDOUT:\n%s', stdout)

        for line in [check for check in stdout.split('\n') if check.strip()]:

            # Ignore duplicate lines (which come up a lot)
            elements = line.split()

            if len(elements) != 5:
                LOG.error('Number of elements unexpected: %i', len(elements))
                LOG.error('xrdfs %s ls -l %s', redirector, path)
                LOG.error(stdout)

            name = elements[-1].split('/')[-1]
            if name in inserted:
                continue
            inserted.add(name)

            mtime = int(
                time.mktime(
                    datetime.datetime.strptime(
                        '%s %s' % (elements[1], elements[2]),
                        '%Y-%m-%d %H:%M:%S').timetuple()
                    )
                )

            # Determine if directory or file
            if elements[0][0] == 'd':
                directories.append((name, mtime))
            else:
                # For files, append tuple (name, size, mtime)
                files.append((name, int(elements[-2]), mtime))

        LOG.info('From %s returning %i directories and %i files.',
                 path, len(directories), len(files))

        LOG.debug('OUTPUT:\n%s\n%s', directories, files)
        return directories, files

    # Create DirectoryInfo for each directory to search
    directories = [
        datatypes.create_dirinfo('/store/', directory, ls_directory) for \
            directory in config.config_dict().get('DirectoryList', [])
        ]

    # Merge the DirectoryInfo
    info = datatypes.DirectoryInfo(name='/store', to_merge=directories)
    info.setup_hash()

    # Save
    info.save(os.path.join(config.config_dict()['CacheLocation'],
                           '%s_remotelisting.pkl' % site))

    # Return the DirectoryInfo
    return info
