"""
A module that provides functions to check the comparison results to
the list of files and deletions in PhEDEx.

:author: Daniel Abercrombie <dabercro@mit.edu>
"""

import time
import logging

from CMSToolBox.webtools import get_json
from . import config
from . import datatypes
from . import cache_tree

LOG = logging.getLogger(__name__)

def set_of_deletions(site):
    """
    Get a list of datasets with approved deletion requests at a given site.

    :param str site: The site that we want the list of deletion requests for.
    :returns: Datasets that are in deletion requests
    :rtype: set
    """

    created_since = int(
        time.time() - config.config_dict().get('InventoryAge', 0) * 24 * 3600)


    # Get deletion requests in PhEDEx
    deletion_request = get_json(
        'cmsweb.cern.ch', '/phedex/datasvc/json/prod/deleterequests',
        {'node': site, 'approval': 'approved', 'create_since': created_since},
        use_https=True)

    # PhEDEx APIs are ridiculous
    # Here I get the dataset names of approved deletion requests in a single list
    datasets_for_deletion = set(
        [block['name'].split('#')[0] for block in sum(
            [request['data']['dbs']['block'] for request in \
                 deletion_request['phedex']['request']],
            [])] + \
        [dataset['name'] for dataset in sum(
            [request['data']['dbs']['dataset'] for request in \
                 deletion_request['phedex']['request']],
            [])]
        ) if deletion_request else set()

    return datasets_for_deletion

@cache_tree('InventoryAge', 'phedexlisting')
def get_phedex_tree(site):
    """
    Get the file list tree from PhEDEx.
    Uses the InventoryAge configuration to determine when to refresh cache.

    :param str site: The site to get information from PhEDEx for.
    :returns: A tree containing file replicas that are supposed to be at the site
    :rtype: ConsistencyCheck.datatypes.DirectoryInfo
    """

    tree = datatypes.DirectoryInfo('/store')

    for ascii_code in range(65, 91):
        dataset = '/%s*/*/*' % chr(ascii_code)
        LOG.info('Getting PhEDEx contents for %s', dataset)

        phedex_response = get_json(
            'cmsweb.cern.ch', '/phedex/datasvc/json/prod/filereplicas',
            {'node': site, 'dataset': dataset},
            use_https=True)

        for block in phedex_response['phedex']['block']:
            tree.add_file_list(
                [(replica['name'], replica['bytes'],
                  int(replica['time_create']), block['name']) \
                     for replica in block['file']])

    return tree
