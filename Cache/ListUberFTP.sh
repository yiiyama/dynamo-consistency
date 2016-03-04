#! /bin/bash

combineCall="cat"

site_SE=SE_$site

UberCall="uberftp -ls gsiftp://${!site_SE}$site_storeLoc"   ## Taken out the -r for now just to test...

echo "Calling site directory structure with:"
echo "$UberCall"

for dir in `cat Directories.txt`
do

    outFile=$fileBase\_$dir.txt

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

    if [ $? -ne 0 ]
    then
        echo "Error making uberftp call for $site. Tried this base:"
        echo $UberCall
        exit 1
    fi

    combineCall="$combineCall $outFile"

    echo "Finished $dir"
done

$combineCall $fileBase.txt

echo "All done with uberftp!"
