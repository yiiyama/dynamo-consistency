#! /bin/bash

# First, make sure that jq is installed

jqCall=./jq                                        # Start out assuming local

if [ ! -f jq ]                                     # If not local, check for installation
then
    if [ "`which jq`" != "" ]
    then
        jqCall=`which jq`
    else                                           # If no installation download locally
        echo ""
        echo "######################################################"
        echo "#                                                    #"
        echo "#     I am copying the tool 'jq' binary locally.     #"
        echo "#     Check out the website here:                    #"
        echo "#     https://stedolan.github.io/jq/                 #"
        echo "#     I know nothing about licensing, but jq         #"
        echo "#     is under the MIT license detailed here:        #"
        echo "#                                                    #"
        echo "# https://github.com/stedolan/jq/blob/master/COPYING #"
        echo "#                                                    #"
        echo "######################################################"
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
fi

# Start by downloading the new files and deletions

for site in `cat SitesList.txt`; do                # Sites you are keeping in this Cache are in SitesList.txt
    if [ "${site:0:1}" = "#" ]                     # Possible to comment out sites
    then
        continue
    fi

    echo $site

    # First, let's look for existing cache, otherwise look for cache on AFS server

    if [ ! -f $site/$site.json ]
    then
        if [ ! -d $site ]
        then
            mkdir $site
        fi
        cacheUrl=http://dabercro.web.cern.ch/dabercro/T2_Cache/$site/$site.json
        wget --spider $cacheUrl
        if [ $? -ne 0 ]
        then
            echo "Looks like the entire content for $site has to be redownloaded..."
            echo "Contact Dan. He hasn't put something nice for this yet because he's a bum."
            exit 1
        else
            wget -O $site/$site.json $cacheUrl
        fi
    fi

#    origtime=`$jqCall -M .phedex.request_timestamp $site/$site\_added.json | sed 's/\..*$//'`
    origtime=`$jqCall -M .phedex.request_timestamp $site/$site.json | sed 's/\..*$//'`
    now=`date +%s`
    oldtime=`expr $now - 302400`                    # Anything older than half a week, time to download
    if [ $oldtime -gt $origtime ]; then
        requesttime=`expr $origtime - 2630000`      # Request data one month older than the previous request
        echo Requesting time: $requesttime

        wget --no-check-certificate -O $site/$site\_added.json https://cmsweb.cern.ch/phedex/datasvc/json/prod/filereplicas?dataset=/*/*/*\&node=$site\&update_since=$requesttime

        # Just temporary until I get this junk working...
        $jqCall -M '.phedex|[.block[]|{directory:.file[0].name|split("/")[0:-2]|join("/"),files:[.file[]|{time:.time_create,adler32:.checksum|split(",")[0]|split(":")[1],file:.name|split("/")[-2:]|join("/"),size:.bytes}],dataset:.name}]' $site/$site.json > $site/$site\_prephedex.json

        $jqCall '[.[].dataset|split("/")[1]]|unique'  $site/$site\_prephedex.json > $site/datasetList.txt

        rm $site/PhEDEx/*.json
        python downloadOld.py -T $site

        $jqCall -M -s '[.[]|.phedex|.block[]|{directory:.file[0].name|split("/")[0:-2]|join("/"),files:[.file[]|{time:.time_create,adler32:.checksum|split(",")[0]|split(":")[1],file:.name|split("/")[-2:]|join("/"),size:.bytes}],dataset:.name}]' $site/PhEDEx/*.json > $site/$site\_prephedex.json

        # Format the data
        $jqCall -M '.phedex|{request_timestamp,block:[.block[]|{directory:.file[0].name|split("/")[0:-2]|join("/"),files:[.file[]|{time:.time_create,adler32:.checksum|split(",")[0]|split(":")[1],file:.name|split("/")[-2:]|join("/"),size:.bytes}],dataset:.name}]}' $site/$site\_added.json > $site/$site\_formatted_added.json
        $jqCall -M -s '.[0] + .[1].block' $site/$site\_prephedex.json $site/$site\_formatted_added.json > $site/$site\_temp.json
        cp $site/$site\_temp.json $site/$site\_prephedex.json
        python mergeFiles.py -T $site
    else
        echo $oldtime' ; '$origtime
    fi
done
