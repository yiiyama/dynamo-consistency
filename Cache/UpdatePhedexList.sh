#! /bin/bash

# Use this thing from IntelROCCS to get reasonable approximation of all datasets
    
DatasetList=$ConsistencyCacheDirectory/DatasetsInPhedexAtSites.dat

getting="wget -O $DatasetList http://t3serv001.mit.edu/~cmsprod/IntelROCCS/Detox/status/DatasetsInPhedexAtSites.dat"

if [ ! -f $DatasetList ]
then
    $getting                                           # I'm going to trust Sid and assume that works
fi

if [ `expr $(date +%s) - $(date +%s -r $DatasetList)` -gt $DatasetsInPhedexAge ]
then
    $getting
fi

for site in `cat SitesList.txt`; do                    # Sites you are keeping in this Cache are in SitesList.txt
    if [ "${site:0:1}" = "#" ]                         # Possible to comment out sites
    then
        continue
    fi

    echo $site

    if [ ! -d $site ]
    then
        mkdir $site
    fi

    if [ -f $site/$site\_temp.json ]
    then
        origtime=`date +%s -r $site/$site\_temp.json`  # This line also assumes you're working on Linux
        now=`date +%s`
        oldtime=`expr $now - $PhedexOutputAge`         # Anything older than half a week, time to download
        if [ $oldtime -lt $origtime ]
        then
            echo "Not updating. File is less than $PhedexOutputAge seconds old."
            echo $oldtime' ; '$origtime
            continue
        fi
    fi

    if [ ! -d $site/PhEDEx ]
    then
        mkdir $site/PhEDEx
    else
        rm $site/PhEDEx/*.json &> /dev/null
    fi
    
    ./downloadOld.py -T $site
    
    $jqCall -M -s '[.[]|.phedex|.block[]|{directory:.file[0].name|split("/")[0:-2]|join("/"),files:[.file[]|{time:.time_create,adler32:.checksum|split(",")[0]|split(":")[1],file:.name|split("/")[-2:]|join("/"),size:.bytes}],dataset:.name}]' $site/PhEDEx/*.json > $site/$site\_prephedex.json

    # Format the data
    $jqCall -M -s '.[0] + .[1].block' $site/$site\_prephedex.json > $site/$site\_temp.json
    cp $site/$site\_temp.json $site/$site\_prephedex.json
    ./mergeFiles.py -T $site                           # I should go back and review what this does
done
