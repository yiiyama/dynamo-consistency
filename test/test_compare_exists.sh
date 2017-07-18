#! /bin/bash

#
# This file checks that test/T2/compare.py exists.
# If it no longer exists, delete this file, and
#
#       !!!UPDATE THE README!!!
#
# for documentation on the production script
#

test -f $(dirname $0)/T2/compare.py || exit 1000
