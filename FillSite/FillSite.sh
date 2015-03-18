#! /bin/bash

SiteName=$1

wget -q -N http://t3serv001.mit.edu/~cmsprod/ConsistencyChecks/FillSite.py                                   # Download FillSite.py, if it's new
wget -q -N http://t3serv001.mit.edu/~cmsprod/ConsistencyChecks/$SiteName/$SiteName\_skipCksm_missing.txt     # Download results list, if it's new

if [ ! -f FillSite.py ]; then
    echo "ERROR: FillSite.py was not successfully downloaded."
    echo "Try checking http://t3serv001.mit.edu/~cmsprod/ConsistencyChecks/FillSite.py"
    exit
fi
if [ ! -f $SiteName\_skipCksm_missing.txt ]; then
    echo "ERROR: ${SiteName}_skipCksm_missing.txt was not successfully downloaded."
    echo "Try checking http://t3serv001.mit.edu/~cmsprod/ConsistencyChecks/$SiteName/${SiteName}_skipCksm_missing.txt"
    exit
fi

echo python FillSite.py -T $SiteName
python FillSite.py -T $SiteName
