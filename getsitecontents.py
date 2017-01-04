"""
Tools to get the files located at a site.

.. Warning::

   Must be used on a site with xrdfs installed.

:author: Daniel Abercrombie <dabercro@mit.edu>
"""


import time
import datetime
import subprocess

from .datatypes import DirectoryInfo


def get_site(site, *dirs):
    """
    Get the information for a site, either from a cache or from XRootD.

    :param str site: The site name
    :param dirs: The list of directories inside ``'/store/'`` to check at the site
    :returns: The site information
    :rtype: DirectoryInfo
    """

    # Get the redirector for a site

    redirector = 'shit'

    if site:
        redirector = 't3serv006.mit.edu'

    # Create the filler function for the DirectoryInfo

    def ls_directory(path):
        """
        Gets the contents of the previously defined redirector at a given path

        :param str path: the full path starting with ``/store/``
        :returns: A list of directories and list of file information
        :rtype: tuple
        """

        directory_listing = subprocess.Popen(['xrdfs', redirector, 'ls', '-l', path],
                                             stdout=subprocess.PIPE,
                                             stderr=subprocess.PIPE)

        directories = []
        files = []
        # For some reason, we have duplicates. Ignore those with this variable
        inserted = set()

        stderr = ''.join(directory_listing.stderr)
        stdout = list(directory_listing.stdout)

        directory_listing.communicate()

        if stderr:
            print stderr
        else:
            for line in stdout:
                # Parse the line to determine if directory or file
                elements = line.split()

                name = elements[-1].split('/')[-1]
                if name in inserted:
                    continue
                inserted.add(name)

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

        return directories, files

    # Create DirectoryInfo for each dirs

    directories = []

    for directory in dirs:
        directories.append(
            DirectoryInfo(location='/store/',
                          name=directory,
                          filler=ls_directory)
            )

    # Merge the DirectoryInfo
    info = DirectoryInfo(name='/store', to_merge=directories)

    # Save
    info.save('/tmp/%s_content.pkl' % site)

    # Optionally dump shit
    info.display()

    # Return the DirectoryInfo
    return info

#--------------------------------------------------------------------------------------------------
#  M A I N  ... for testing for now
#--------------------------------------------------------------------------------------------------

# LAST CHARACTER IN THE DIR HAS TO BE A   '/'

if __name__ == '__main__':

    get_site('T3_US_MIT', 'mc')
