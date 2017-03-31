#! /usr/bin/env python

import logging

from compare import main

if __name__ == '__main__':

    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s:%(levelname)s:%(name)s: %(message)s')

    sites = [
#        'T3_US_MIT',
        'T2_US_MIT',
#        'T2_US_Nebraska',
#        'T2_US_Florida',
#        'T2_US_Purdue',
#        'T2_US_UCSD',
        'T2_US_Vanderbilt',
        'T1_US_FNAL',
#        'T2_US_Wisconsin',
#        'T2_US_Caltech',
        ]

    for site in sites:
        main(site)
