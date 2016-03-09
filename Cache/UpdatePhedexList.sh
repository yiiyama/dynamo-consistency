#! /bin/bash

# Use this thing from IntelROCCS to get reasonable approximation of all datasets
    
DatasetList=$ConsistencyCacheDirectory/DatasetsInPhedexAtSites.dat
DatasetForSite=$ConsistencyCacheDirectory/$site/PhEDEx/CheckThese.txt

getting="wget -O $DatasetList http://t3serv001.mit.edu/~cmsprod/IntelROCCS/Detox/status/DatasetsInPhedexAtSites.dat"  # Get the dataset list if a new one is available

if [ ! -f $DatasetList ]
then
    $getting                                       # I'm going to trust Sid and assume that works
fi

if [ `expr $(date +%s) - $(date +%s -r $DatasetList)` -gt $DatasetsInPhedexAge ]
then
    $getting
fi

if [ ! -d $ConsistencyCacheDirectory/$site/PhEDEx ]
then
    mkdir $ConsistencyCacheDirectory/$site/PhEDEx
fi

# Merge DatasetsInPhedexAtSites.dat datasets with sets already checked for at site (which can be empty/reset once in a while)

cat <(cat $fileBase\_addData.txt 2> /dev/null) <(grep $site $DatasetList | awk -F[/] '{print $2}') | sort | uniq > $DatasetForSite

now=`date +%s`
oldtime=`expr $now - $PhedexOutputAge`         # Anything older than half a week, time to download

for dataset in `cat $DatasetForSite`
do
    outputTarget=$ConsistencyCacheDirectory/$site/PhEDEx/$dataset.json
    getting="wget --no-check-certificate -O $outputTarget https://cmsweb.cern.ch/phedex/datasvc/json/prod/filereplicas?dataset=/$dataset/*/*&node=$site"

    if [ ! -f $outputTarget ]                # If the desired file does not exist, then download
    then
        $getting
    fi

    origtime=`date +%s -r $outputTarget`     # This line also assumes you're working on Linux

    if [ $origtime -lt $oldtime ]            # if the target file is older than the maximum age for PhEDEx output, then download
    then
        $getting
    fi
done

# Combine and format the data

$jqCall -M -s '[.[]|.phedex|.block[]|{directory:.file[0].name|(split("/")[0:-2]|env.site_storeLoc + join("/") + "/"),files:[.file[]|{time:.time_create,adler32:.checksum|split(",")[0]|split(":")[1],file:.name|split("/")[-2:]|join("/"),size:.bytes}],dataset:.name}]' $ConsistencyCacheDirectory/$site/PhEDEx/*.json > $ConsistencyCacheDirectory/$site/$site\_phedex.json
