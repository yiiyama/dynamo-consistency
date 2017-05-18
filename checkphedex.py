# pylint: disable=import-error

"""
A module that provides functions to check the comparison results to
the list of files and deletions in PhEDEx.

:author: Daniel Abercrombie <dabercro@mit.edu>
"""

import time
import logging

from common.interface.mysql import MySQL

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

    valid_list = config.config_dict().get('DirectoryList', [])

    sql = MySQL(config_file='/etc/my.cnf', db='dynamo', config_group='mysql-dynamo')
    datasets = sql.query('SELECT datasets.name '
                         'FROM sites INNER JOIN dataset_replicas INNER JOIN datasets '
                         'WHERE dataset_replicas.dataset_id=datasets.id AND '
                         'dataset_replicas.site_id=sites.id and sites.name=%s', site)

    def add_files(dataset, retries):
        """
        :param str dataset: Dataset to get from PhEDEx
        :param int retries: The number of times to retry PhEDEx call
        :returns: Whether or not the addition was successful
        :rtype: bool
        """

        LOG.info('Getting PhEDEx contents for %s', dataset)

        phedex_response = get_json(
            'cmsweb.cern.ch', '/phedex/datasvc/json/prod/filereplicas',
            {'node': site, 'dataset': dataset},
            retries=retries,
            use_https=True)

        report = 0

        if not phedex_response:
            LOG.warning('Bad response from PhEDEx for %s', dataset)
            return False

        for block in phedex_response['phedex']['block']:
            LOG.debug('%s', block)
            replica_list = [(replica['name'], replica['bytes'],
                             int(replica['replica'][0]['time_create'] or time.time()),
                             block['name']) \
                                for replica in block['file'] \
                                if replica['name'].split('/')[2] in valid_list]

            report += len(replica_list)

            tree.add_file_list(replica_list)

        LOG.info('%i files', report)
        return True

    separate = []

    for primary in set([d.split('/')[1][:3] for d in datasets]):
        success = add_files('/%s*/*/*' % primary, 0)
        if not success:
            separate.append(primary)

    # Separate loop to retry datasets individually
    for dataset in [d for d in datasets if d.split('/')[1][:3] in separate]:
        success = add_files(dataset, 5)
        if not success:
            LOG.critical('Cannot get %s from PhEDEx. Do not trust results...', dataset)

    return tree
