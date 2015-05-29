#! /bin/bash

# Start by downloading the new files and deletions

for site in `ls -d T2_*/ | sed 's/[/].*$//'`; do
    echo $site

    # First, let's generate a time that we are interested in looking at
    origtime=`/home/dabercro/./jq -M .phedex.request_timestamp $site/$site\_added.json | sed 's/\..*$//'`
    now=`date +%s`
    oldtime=`expr $now - 30` #2400`                    # Anything older than half a week, time to download
    if [ $oldtime -gt $origtime ]; then
        requesttime=`expr $origtime - 604800`       # Request data one week older than the previous request
        requestdelete=`expr $origtime - 907200`     # Request deletions 1.5 weeks older than the previous request
        echo Requesting time: $requesttime

#        wget --no-check-certificate -O $site/$site\_added.json https://cmsweb.cern.ch/phedex/datasvc/json/prod/filereplicas?dataset=/*/*/*\&node=$site\&update_since=$requesttime\&complete=y
#        wget --no-check-certificate -O $site/$site\_deleted.json https://cmsweb.cern.ch/phedex/datasvc/json/prod/deletions?dataset=/*/*/*\&node=$site\&request_since=$requestdelete\&complete=y

        # Just temporary until I get this junk working...
        /home/dabercro/./jq -M '.phedex|{request_timestamp,block:[.block[]|{directory:.file[0].name|split("/")[0:-1]|join("/"),files:[.file[]|{time:.time_create,adler32:.checksum|split(",")[0]|split(":")[1],file:.name|split("/")[-1:][0],size:.bytes}],dataset:.name}]}' $site/$site.json > $site/$site\_prephedex.json

        # Format the data
        /home/dabercro/./jq -M '.phedex|{request_time:.request_timestamp,block:[.dataset[].block[0]|{name,time:.deletion[0].time_complete}]}' $site/$site\_deleted.json > $site/$site\_formatted_deleted.json
        /home/dabercro/./jq -M '.phedex|{request_timestamp,block:[.block[]|{directory:.file[0].name|split("/")[0:-1]|join("/"),files:[.file[]|{time:.time_create,adler32:.checksum|split(",")[0]|split(":")[1],file:.name|split("/")[-1:][0],size:.bytes}],dataset:.name}]}' $site/$site\_added.json > $site/$site\_formatted_added.json
        /home/dabercro/./jq -M -s '.[0].block + .[1].block' $site/$site\_prephedex.json $site/$site\_formatted_added.json > $site/$site\_temp.json
        cp $site/$site\_temp.json $site/$site\_prephedex.json
        python mergeFiles.py -T $site
    fi
done
