"""
A module that provides functions to check the comparison results to
the list of files and deletions in PhEDEx.

:author: Daniel Abercrombie <dabercro@mit.edu>
"""

import time

from CMSToolBox.webtools import get_json
from . import config

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
