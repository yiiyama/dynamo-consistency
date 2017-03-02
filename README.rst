ConsistencyCheck
================

|build|

Compares files on site to files in the dynamo inventory.
This tool requires ``dynamo`` and ``xrdfs`` to be installed separately.

Then a configuration file should be created.

.. autoanysrc:: phony
   :src: ../ConsistencyCheck/test/config.yml
   :analyzer: shell-script

A check can be done simply by doing the following::

    from ConsistencyCheck import config, datatypes, getsitecontents, getinventorycontents

    config.CONFIG_FILE = '/path/to/config.json'
    site = 'T2_US_MIT'     # For example

    inventory_listing = getinventorycontents.get_inventory_tree(site)
    remote_listing = getsitecontents.get_site_tree(site)

    datatypes.compare(inventory_listing, remote_listing, 'results')

In this example,
the list of file LFNs in the inventory and not at the site will be in ``results_missing.txt``.
The list of file LFNs at the site and not in the inventory will be in ``results_orphan.txt``.

Reference
+++++++++

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
