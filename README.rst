ConsistencyCheck
================

|build|

.. contents:: :local:

Compares files on site to files in the dynamo inventory.
This tool requires ``dynamo`` and ``xrdfs`` to be installed separately.

.. _consistency-config-ref:

Running the Tool
++++++++++++++++

A consistency check on a site can be done simply by doing the following::

    from ConsistencyCheck import config, datatypes, getsitecontents, getinventorycontents

    config.CONFIG_FILE = '/path/to/config.json'
    site = 'T2_US_MIT'     # For example

    inventory_listing = getinventorycontents.get_inventory_tree(site)
    remote_listing = getsitecontents.get_site_tree(site)

    datatypes.compare(inventory_listing, remote_listing, 'results')

In this example,
the list of file LFNs in the inventory and not at the site will be in ``results_missing.txt``.
The list of file LFNs at the site and not in the inventory will be in ``results_orphan.txt``.

Configuration
+++++++++++++

A configuration file should be created before pointing to it, like above.
The configuration file for ConsistencyChecks is a JSON or YAML file with the following keys

.. autoanysrc:: phony
   :src: ../ConsistencyCheck/test/config.yml
   :analyzer: shell-script

Configuration parameters can also be quickly overwritten for a given run by
setting an environment variable of the same name.

Production Settings
+++++++++++++++++++

The configuration in production is the following.

.. program-output:: cat ../ConsistencyCheck/prod/consistency_config.json

.. Note::
   The following script description was last updated on July 19, 2017.

The production script,
located at ``ConsistencyCheck/prod/compare.py`` at the time of writing,
goes through the following steps for each site.

  #. It gathers the site tree by calling :py:func:`ConsistencyCheck.getsitecontents.get_site_tree()`.
  #. It gathers the inventory tree by calling :py:func:`ConsistencyCheck.getinventorycontents.get_db_listing()`.
  #. It creates a list of datasets to not report orphans in.
     This list consists of the following.

     - Deletion requests fetched from PhEDEx by :py:func:`ConsistencyCheck.checkphedex.set_of_deletetion()`
     - A dataset that has any files on the site, as listed by the dynamo MySQL database
     - Any datasets that have the status flag set to ``'IGNORED'`` in the dynamo database
     - Datasets merging datasets that are
       `protected by Unified <https://cmst2.web.cern.ch/cmst2/unified/listProtectedLFN.txt>`_

  #. Does the comparison between the two trees made.
     (Keep in mind the configuration options listed under
     :ref:`consistency-config-ref` concerning file age.)
  #. Connects to a dynamo registry to report errors.
     At the moment, if the site is ``'T2_US_MIT'``,
     this connection is made to Max's development server.
     Otherwise, the connection is to the production dynamo database.
  #. For each missing file, every possible source site as listed by the dynamo database,
     (not counting the site where missing), is entered in the transfer queue.
  #. Every orphan file and every empty directory that is not too new is entered in the deletion queue.

     .. Warning::
        The production script no longer cleans out site entries in the deletion or transfer queues.
        Some other tool is expected to handle that.

  #. Creates a text file that contains the missing blocks and groups.
  #. ``.txt`` file lists and details of orphan and missing files are moved to the web space
     and the stats database is updated.

Reference
+++++++++

The following is a full reference to the submodules inside of the :py:mod:`ConsistencyCheck` module.

config.py
---------

.. automodule:: ConsistencyCheck.config
   :members:

datatypes.py
------------

.. automodule:: ConsistencyCheck.datatypes
   :members:

getsitecontents.py
------------------

.. automodule:: ConsistencyCheck.getsitecontents
   :members:

getinventorycontents.py
-----------------------

.. automodule:: ConsistencyCheck.getinventorycontents
   :members:

.. |build| image:: https://travis-ci.org/dabercro/ConsistencyCheck.svg?branch=master
    :target: https://travis-ci.org/dabercro/ConsistencyCheck
