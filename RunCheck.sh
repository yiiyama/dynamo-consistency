#! /bin/bash

export ConsistencyDir=`pwd`

source Config/ConsistencyConfig.sh

# First, make sure that uberftp is installed

if [ "`which uberftp 2> /dev/null`" = "" ]
then
    echo "uberftp not installed on this machine."
    echo "What do you think you're doing?"
    exit 1
fi

# Then, make sure that jq is installed. If not, install it

jqCall=`which jq 2> /dev/null`

if [ "$jqCall" = "" ]
then
    if [ ! -f jq ]                                 # If not local, check for installation
    then
        echo ""
        echo " ######################################################"
        echo " #                                                    #"
        echo " #     I am copying the tool 'jq' binary locally.     #"
        echo " #     Check out the website here:                    #"
        echo " #     https://stedolan.github.io/jq/                 #"
        echo " #     I know nothing about licensing, but jq         #"
        echo " #     is under the MIT license detailed here:        #"
        echo " #                                                    #"
        echo " # https://github.com/stedolan/jq/blob/master/COPYING #"
        echo " #                                                    #"
        echo " ######################################################"
        echo ""

        downloadUrl=https://github.com/stedolan/jq/releases/download/jq-1.5
        if [ "`uname -m`" = "x86_64" ]             # Check for architecture
        then
            downloadUrl=$downloadUrl/jq-linux64    # I'm assuming linux binaries work for now
        else
            downloadUrl=$downloadUrl/jq-linux32
        fi
        wget -O jq $downloadUrl                    # Download jq and install locally
        chmod +x jq
    fi
    jqCall=$ConsistencyDir/jq
fi

export jqCall

# Make sure the webpages are up to date.

if [ ! -d $ConsistencyWebpages ]
then
    mkdir -p $ConsistencyWebpages
fi

cp webpages/*.html $ConsistencyWebpages/.

# Then do stuff for each site.

for site in `cat Config/SitesList.txt`
do

    if [ "${site:0:1}" = "#" ]               # Possible to comment out sites
    then
        continue
    fi

    export site
    export fileBase=$ConsistencyCacheDirectory/$site/$site

    # First check for Cache file directory

    if [ ! -d $ConsistencyCacheDirectory/$site ]
    then
        mkdir -p $ConsistencyCacheDirectory/$site
    fi

    # Check for lfn2pfn file

    if [ ! -f ${fileBase}_lfn2pfn.json ]
    then
        wgetCall="wget --no-check-certificate -O ${fileBase}_lfn2pfn.json https://cmsweb.cern.ch/phedex/datasvc/json/prod/lfn2pfn?node=$site&protocol=direct&lfn=/store/data/test.root"
        $wgetCall
        if [ $? -ne 0 ]
        then
            echo "Error getting LFN2PFN for $site. Tried this:"
            echo "$wgetCall"
            exit 1
        fi
    fi

    export site_storeLoc=`$jqCall '.phedex.mapping[0]|.pfn|split("/store/data/test.root")[0]' ${fileBase}_lfn2pfn.json | sed 's/"//g'`

    echo " ### $site ###"

    Cache/UpdatePhedexList.sh &
    Cache/ListUberFTP.sh

    if [ $? -ne 0 ]             # if ListUberFTP.sh fails, then we give up for now
    then
        echo "Was not able to connect uberftp to $site"
        kill $!                 # This kills the forked process
        exit 1
    fi

    Cache/ConvertText.py        # Convert the ListUberFTP.sh output to phedex-style JSOn

    wait                        # Wait for forked process if needed.

    Check/ConsistencyCheck.py

    DoubleCheck/CheckRemovable.sh

    if [ $? -ne 0 ]
    then
        Cache/UpdatePhedexList.sh
        Check/ConsistencyCheck.py
    fi

    if [ ! -d $ConsistencyWebpages/$site ]
    then
        mkdir -p $ConsistencyWebpages/$site
    fi

    cp $fileBase*_missing.txt  $fileBase*_removable.txt  $fileBase*_summary.txt $ConsistencyWebpages/$site

    crab/updateList.py -D $ConsistencyWebpages

done
