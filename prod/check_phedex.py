#! /usr/bin/env python

import sys

from ConsistencyCheck.checkphedex import check_for_datasets

if __name__ == '__main__':

    bad = check_for_datasets(sys.argv[1], '%s_compare_orphan.txt' % sys.argv[1])

    print ''

    for num, dataset in bad:
        print num, dataset
