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
export oldtime=`expr $now - $PhedexOutputAge`         # Anything older than half a week, time to download

if [ `cat "$DatasetForSite" | wc -l` -eq 0 ]
then
    cp Cache/default.txt $DatasetForSite
fi

# cat "$DatasetForSite" | xargs -n1 -P$NumPhedexThreads Cache/DownloadPhedex.sh ## This is hard to kill. Put back when stable.

for dataset in `cat "$DatasetForSite"`
do
    Cache/DownloadPhedex.sh $dataset
done

# Combine and format the data

$jqCall -M -s '[.[]|.phedex|.block[]|{directory:.file[0].name|(split("/")[0:-4]|env.site_storeLoc + join("/") + "/"),files:[.file[]|{time:.time_create,adler32:.checksum|split(",")[0]|split(":")[1],file:.name|split("/")[-4:]|join("/"),size:.bytes}],dataset:.name}]' $ConsistencyCacheDirectory/$site/PhEDEx/*.json > $ConsistencyCacheDirectory/$site/$site\_phedex.json
