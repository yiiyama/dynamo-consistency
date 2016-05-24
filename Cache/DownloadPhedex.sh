#!/bin/bash

# This is just a wrapper to download files from Phedex in parallel

dataset=$1

echo $dataset" called for"

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
