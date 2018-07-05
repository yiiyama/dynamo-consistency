Running Over CMS Sites
======================

.. _consistency-config-ref:

Configuration
+++++++++++++

A configuration file should be created before pointing to it, like above.
The configuration file for Site Consistency is a JSON or YAML file with the following keys

.. autoanysrc:: phony
   :src: ../test/config.yml
   :analyzer: shell-script

Configuration parameters can also be quickly overwritten for a given run by
setting an environment variable of the same name.

Production Settings
-------------------

The configuration in production is the following.

.. program-output:: cat ../prod/consistency_config.json

.. _compare-ref:

Comparison Script
+++++++++++++++++

.. automodule:: compare

Automatic Site Selection
------------------------

To automatically run ``prod/compare.py`` over a few well-deserving sites, use ``prod/run_checks.sh``.

.. autoanysrc:: phony
   :src: ../prod/run_checks.sh
   :analyzer: perl-script

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
