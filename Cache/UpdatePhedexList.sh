#! /bin/bash

# Use this thing from IntelROCCS to get reasonable approximation of all datasets
    
DatasetList=$ConsistencyCacheDirectory/DatasetsInPhedexAtSites.dat

getting="wget -O $DatasetList http://t3serv001.mit.edu/~cmsprod/IntelROCCS/Detox/status/DatasetsInPhedexAtSites.dat"

if [ ! -f $DatasetList ]
then
    $getting                                       # I'm going to trust Sid and assume that works
fi

if [ `expr $(date +%s) - $(date +%s -r $DatasetList)` -gt $DatasetsInPhedexAge ]
then
    $getting
fi

if [ ! -d $site/PhEDEx ]
then
    mkdir $site/PhEDEx
fi

# Merge DatasetsInPhedexAtSites.dat datasets with sets already checked for at site (which can be empty/reset once in a while)

cat <(cat $site/CheckThese.txt 2> /dev/null) <(grep $site DatasetsInPhedexAtSites.dat | awk -F[/] '{print $2}') | sort | uniq > $site/CheckThese.txt

now=`date +%s`
oldtime=`expr $now - $PhedexOutputAge`         # Anything older than half a week, time to download

for dataset in `cat $site/CheckThese.txt`
do
    outputTarget=$site/PhEDEx/$dataset.json
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

$jqCall -M -s '[.[]|.phedex|.block[]|{directory:.file[0].name|split("/")[0:-2]|join("/"),files:[.file[]|{time:.time_create,adler32:.checksum|split(",")[0]|split(":")[1],file:.name|split("/")[-2:]|join("/"),size:.bytes}],dataset:.name}]' $site/PhEDEx/*.json > $site/$site\_prephedex.json

## Format the data
#$jqCall -M -s '.[0] + .[1].block' $site/$site\_prephedex.json > $site/$site\_temp.json
#cp $site/$site\_temp.json $site/$site\_prephedex.json
#./mergeFiles.py -T $site     # Replace this step or combine with above
