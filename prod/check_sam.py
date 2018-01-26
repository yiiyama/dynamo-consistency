#! /usr/bin/env python

import sys
import sqlite3

from CMSToolBox.samstatus import is_sam_good

if __name__ == '__main__':
    database = sys.argv[1]
    sites = sys.argv[2:]

    conn = sqlite3.connect(database)
    curs = conn.cursor()

    for site in sites:
        if 'T2' in site:
            try:
                if not is_sam_good(site):
                    curs.execute('UPDATE sites SET isrunning = -1 WHERE site = ? AND isrunning = 0', (site, ))
            except:
                print '%s not in SAM tests!' % site

    conn.commit()
    conn.close()
