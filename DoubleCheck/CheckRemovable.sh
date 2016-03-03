#! /bin/bash

SiteName=$1

if [ "$SiteName" = "" ]
then
    echo "Please give Site Name, please."
    exit 0
fi

if [ ! -f ../ConsistencyCheck/$SiteName\_skipCksm_removable.txt ] || [ ! -f ../ConsistencyCheck/$SiteName\_phedex.json ]
then
    echo "Output files are missing for $SiteName"
    exit 1
fi

for checking in `cat ../ConsistencyCheck/$SiteName\_skipCksm_removable.txt`
do
    if [ "${checking:0:1}" != "/" ]
    then
        continue
    fi

    if [ "`grep $checking ../ConsistencyCheck/$SiteName\_phedex.json`" != "" ]
    then
        echo "Found $checking"
        echo "Exiting..."
        exit 1
    else
        ./CheckForPhEDEx.py $SiteName $checking
    fi
done

exit 0
