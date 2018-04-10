"""
config file for use by:
http://cms-comp-ops-tools.readthedocs.io/en/latest/siteadmintoolkit.html#unmerged-cleaner
"""

LFN_TO_CLEAN = '/store/unmerged'
UNMERGED_DIR_LOCATION = LFN_TO_CLEAN
WHICH_LIST = 'files'
# Updated by compare.py
DELETION_FILE = '_unmerged.txt'
SLEEP_TIME = 0.5
DIRS_TO_AVOID = ['SAM', 'logs']
# The default (1209600) corresponds to two weeks.
MIN_AGE = 1209600 * 2
STORAGE_TYPE = 'posix'
