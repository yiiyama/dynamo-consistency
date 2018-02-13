#! /usr/bin/env python
# pylint: disable=bare-except

"""
A script that updates the summary database to not run on sites failing SAM tests lately

:author: Daniel Abercrombie <dabercro@mit.edu>
"""

import sys
import sqlite3

from CMSToolBox.samstatus import is_sam_good

def main(database, sites):
    """
    Updates the database to not run on sites with bad SAM tests

    :param str database: Location of the database file
    :param list sites: List of all of the sites to check
    """

    conn = sqlite3.connect(database)
    curs = conn.cursor()

    for site in sites:
        if 'T2' in site:
            try:
                if not is_sam_good(site):
                    curs.execute(
                        'UPDATE sites SET isrunning = -1 WHERE site = ? AND isrunning = 0',
                        (site, )
                        )
            except:
                print '%s not in SAM tests!' % site

    conn.commit()
    conn.close()

if __name__ == '__main__':
    main(database=sys.argv[1], sites=sys.argv[2:])
