Site Consistency
================

|build|

.. contents:: :local:
   :depth: 2

Compares files on site to files in the dynamo inventory.
This tool requires ``dynamo`` and ``xrdfs`` to be installed separately.

Running the Tool
++++++++++++++++

A simple consistency check on a site can be done by doing the following
when an instance of ``dynamo`` is installed::

    from dynamo_consistency import config, datatypes, getsitecontents, getinventorycontents

    config.CONFIG_FILE = '/path/to/config.json'
    site = 'T2_US_MIT'     # For example

    inventory_listing = getinventorycontents.get_inventory_tree(site)
    remote_listing = getsitecontents.get_site_tree(site)

    datatypes.compare(inventory_listing, remote_listing, 'results')

In this example,
the list of file LFNs in the inventory and not at the site will be in ``results_missing.txt``.
The list of file LFNs at the site and not in the inventory will be in ``results_orphan.txt``.

The actual comparison done by the production instance of ``dynamo`` has a few more filters and steps,
as outlined under :ref:`compare-ref`.

.. _consistency-config-ref:

Configuration
+++++++++++++

A configuration file should be created before pointing to it, like above.
The configuration file for Site Consistency is a JSON or YAML file with the following keys

.. autoanysrc:: phony
   :src: ../dynamo-consistency/test/config.yml
   :analyzer: shell-script

Configuration parameters can also be quickly overwritten for a given run by
setting an environment variable of the same name.

Production Settings
-------------------

The configuration in production is the following.

.. program-output:: cat ../dynamo-consistency/prod/consistency_config.json

.. _compare-ref:

Comparison Script
+++++++++++++++++

.. automodule:: compare

Automatic Site Selection
------------------------

To automatically run ``prod/compare.py`` over a few well-deserving sites, use ``prod/run_checks.sh``.

.. autoanysrc:: phony
   :src: ../dynamo-consistency/prod/run_checks.sh
   :analyzer: perl-script

Moving Sites To and From Debugged Tab
+++++++++++++++++++++++++++++++++++++

To mark sites as ready to be acted on,
change the ``isgood`` value in the ``sites`` table in the summary database to ``1``.
For example, if you are in the directory of your webpage,
and want to mark ``T2_US_MIT`` as good, you could do the following::

    echo "UPDATE sites SET isgood = 1 WHERE site = 'T2_US_MIT';" | sqlite3 stats.db

To mark a site as bad, set ``isgood`` to ``0``::

    echo "UPDATE sites SET isgood = 0 WHERE site = 'T2_US_MIT';" | sqlite3 stats.db

Checking PhEDEx for Dataset Presence
------------------------------------

.. automodule:: check_phedex

Manually Setting XRootD Doors
+++++++++++++++++++++++++++++

In addition to the **Redirectors** key in the configuration file, which sets the redirector for a site,
there is also a mechanism for setting all the doors for a site.
A list of possible doors can be found at ``<CacheLocation>/<SiteName>_redirector_list.txt``.
Any url in that list that matches the domain of the site will be used to make ``xrootd`` calls.
To add or remove urls from this list, just add or remove lines from this file.

.. Note::
   If the **RedirectorAge** configuration parameter is not set to ``0``,
   then this redirector list will be overwritten once it becomes too old.
   To force the generation of a new list when the **RedirectorAge** is set to ``0``,
   simply delete the redirector list file for that site.

A list of redirectors found by the global redirectors is stored in ``<CacheLocation>/redirector_list.txt``.

Reference
+++++++++

The following is a full reference to the submodules inside of the :py:mod:`dynamo_consistency` module.

checkphedex.py
--------------

.. automodule:: dynamo_consistency.checkphedex
   :members:

.. _config-ref:

config.py
---------

.. automodule:: dynamo_consistency.config
   :members:

datatypes.py
------------

.. automodule:: dynamo_consistency.datatypes
   :members:

getsitecontents.py
------------------

.. automodule:: dynamo_consistency.getsitecontents
   :members:

getinventorycontents.py
-----------------------

.. automodule:: dynamo_consistency.getinventorycontents
   :members:

.. |build| image:: https://travis-ci.org/SmartDataProjects/dynamo-consistency.svg?branch=master
   :target: https://travis-ci.org/SmartDataProjects/dynamo-consistency
