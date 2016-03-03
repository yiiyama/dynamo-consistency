#! /bin/bash

SiteName=$1

if [ "$SiteName" = "" ]
then
    echo "Please give Site Name, please."
    exit 0
fi

if [ ! -f ../$SiteName\_skipCksm_removable.json ] || [ ! -f ../$SiteName\_phedex.json ]
then
    echo "Output files are missing for $SiteName"
    exit 1
fi

for checking in `cat ../$SiteName\_skipCksm_removable.json`
do
    if [ "`grep $checking ../$SiteName\_phedex.json`" != "" ]
    then
        echo "Found $checking in ../$SiteName\_phedex.json!"
        echo "Exiting..."
        exit 1
    fi
done

exit 0
