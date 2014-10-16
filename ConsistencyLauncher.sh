#! /bin/bash

TName=$1
Skip=$2

StartTime=$(date +%s)

if [ ! -f $TName.json ]; then
    wget --no-check-certificate -O $TName.json https://cmsweb.cern.ch/phedex/datasvc/json/prod/filereplicas?dataset=/*/*/*\&node=$TName
fi
if [ ! -f $TName\_tfc.json ]; then
    wget --no-check-certificate -O $TName\_tfc.json https://cmsweb.cern.ch/phedex/datasvc/json/prod/tfc?node=$TName
fi

echo 'Have all JSON files from PhEDEx.'
echo 'Starting python script...'

if [ "$Skip" == "doCksm" ]; then 
    python ConsistencyCheck.py -T $TName
else
    python ConsistencyCheck.py -T $TName -s
fi

EndTime=$(date +%s)

echo 'Elapsed time is approximately '$((EndTime-StartTime))' seconds.'
