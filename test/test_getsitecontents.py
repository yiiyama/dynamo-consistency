#! /usr/bin/env python

import sys
import logging

import ConsistencyCheck.getsitecontents as gsc

if __name__ == '__main__':

    logging.basicConfig(level=logging.INFO)

    if len(sys.argv) == 2:
        gsc.get_site(sys.argv[1])
    else:
        gsc.get_site('T3_US_MIT')
