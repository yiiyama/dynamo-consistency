#! /bin/bash

# Start by downloading the new files and deletions

for site in `ls -d T2_*/ | sed 's/[/].*$//'`; do
    echo $site

    # First, let's generate a time that we are interested in looking at
    #############################################################################################
    # origtime=`/home/dabercro/./jq .phedex.request_timestamp $site/$site.json | sed 's/\..*$//'`
    # requesttime=`expr $origtime - 604800`       # Request data one week older than the previous request
    # echo Requesting time: $requesttime
    #
    # wget --no-check-certificate -O $site/$site\_added.json https://cmsweb.cern.ch/phedex/datasvc/json/prod/filereplicas?dataset=/*/*/*\&node=$site\&update_since=$requesttime
    # wget --no-check-certificate -O $site/$site\_deleted.json https://cmsweb.cern.ch/phedex/datasvc/json/prod/deletions?dataset=/*/*/*\&node=$site\&request_since=$requesttime
    #############################################################################################
    # Uncomment all that later


done


