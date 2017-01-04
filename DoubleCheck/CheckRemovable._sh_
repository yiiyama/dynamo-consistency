#! /bin/bash

numAdded=0

if [ ! -f $fileBase\_skipCksm_removable.txt ] || [ ! -f $fileBase\_phedex.json ]
then
    echo "Output files are missing for $site"
    exit 1
fi

for checking in `cat $fileBase\_skipCksm_removable.txt`
do
    if [ "${checking:0:1}" != "/" ]
    then
        continue
    fi

    if [ "`grep $checking $fileBase\_phedex.json`" != "" ]
    then
        echo "Found $checking"
        echo "Exiting..."
        exit 1
    else
        DoubleCheck/CheckForPhEDEx.py $checking
        if [ $? -eq 1 ]
        then
            cat $fileBase\_addData.txt >> $ConsistencyCacheDirectory/$site/PhEDEx/CheckThese.txt
            numAdded=$((numAdded + 1))
        fi
    fi
done

exit $numAdded
