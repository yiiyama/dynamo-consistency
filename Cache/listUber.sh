#! /bin/bash

for dir in `cat Directories.txt`
do
    if [ ! -f T2_CH_CSCS_$dir.txt ]
    then
        uberftp -ls -r gsiftp://storage01.lcg.cscs.ch/pnfs/lcg.cscs.ch/cms/trivcat/store/$dir > T2_CH_CSCS_$dir.txt
        echo "Finished $dir"
    else
        echo "$dir already saved!"
    fi
done

echo "All done!"
