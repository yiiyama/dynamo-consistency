#! /bin/bash

combineCall="cat"

site_SE=SE_$site

UberCall="uberftp -ls -r gsiftp://${!site_SE}$site_storeLoc"

# Call without recursion just to test

uberftp -ls gsiftp://${!site_SE}$site_storeLoc > /dev/null

if [ $? -ne 0 ]
then
    echo "Error making uberftp call for $site. Tried this base:"
    echo $UberCall
    exit 1
fi

echo "Calling site directory structure with:"
echo "$UberCall"

for dir in `cat $ConsistencyDir/Config/Directories.txt`
do

    outFile=${fileBase}_$dir.txt

    combineCall="$combineCall $outFile"

    # Check if the output file exists
    
    if [ -f $outFile ]
    then

        # Check if the file is old

        origtime=`date +%s -r $outFile`
        now=`date +%s`
        oldtime=`expr $now - $UberftpOutputAge`
        if [ $oldtime -lt $origtime ]
        then
            echo "Not updating. $outFile is less than $UberftpOutputAge seconds old."
            echo $oldtime' ; '$origtime
            continue
        fi
    fi

    $UberCall/$dir > $outFile

    echo "Finished $dir"
done

$combineCall > $fileBase.txt

echo "All done with uberftp!"

exit 0
