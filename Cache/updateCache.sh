#! /bin/bash

# Start by downloading the new files and deletions

for site in `ls -d T2_*/ | sed 's/[/].*$//'`; do
    echo $site

    # First, let's generate a time that we are interested in looking at
    origtime=`/home/dabercro/./jq -M .phedex.request_timestamp $site/$site.json | sed 's/\..*$//'`
    now=`date +%s`
    oldtime=`expr $now - 302400`                    # Anything older than half a week, time to download
    if [ $oldtime -gt $origtime ]; then
#        requesttime=`expr $origtime - 604800`       # Request data one week older than the previous request
        echo Requesting time: $requesttime

#        wget --no-check-certificate -O $site/$site\_added.json https://cmsweb.cern.ch/phedex/datasvc/json/prod/filereplicas?dataset=/*/*/*\&node=$site\&update_since=$requesttime\&complete=y
#        wget --no-check-certificate -O $site/$site\_deleted.json https://cmsweb.cern.ch/phedex/datasvc/json/prod/deletions?dataset=/*/*/*\&node=$site\&request_since=$requesttime\&complete=y

        # Format the data
        /home/dabercro/./jq -M '.phedex|{request_time:.request_timestamp,block:[.dataset[].block[0]|{name,time:.deletion[0].time_complete}]}' $site/$site\_deleted.json > $site/$site\_formatted_deleted.json
        /home/dabercro/./jq -M -s '.[0] * .[1] | .phedex|{request_timestamp,block:[.block[]|{directory:.file[0].name|split("/")[0:-1]|join("/"),files:[.file[]|{time:.time_create,alder32:.checksum|split(",")[0]|split(":")[1],file:.name|split("/")[-1:][0],size:.bytes}],dataset:.name}]}' $site/$site.json $site/$site\_added.json > $site/$site\_formatted_added.json
        python mergeFiles.py -T $site
    fi
done
