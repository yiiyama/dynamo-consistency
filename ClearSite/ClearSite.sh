#! /bin/bash

SiteName=$1

wget -q -N http://t3serv001.mit.edu/~cmsprod/ConsistencyChecks/ClearSite.py                                  # Download ClearSite.py, if it's new
wget -q -N http://t3serv001.mit.edu/~cmsprod/ConsistencyChecks/$SiteName/$SiteName\_skipCksm_removable.txt   # Download results list, if it's new

if [ ! -f ClearSite.py ]; then
    echo "ERROR: ClearSite.py was not successfully downloaded."
    echo "Try checking http://t3serv001.mit.edu/~cmsprod/ConsistencyChecks/ClearSite.py"
    exit
fi
if [ ! -f $SiteName\_skipCksm_removable.txt ]; then
    echo "ERROR: ${SiteName}_skipCksm_removable.txt was not successfully downloaded."
    echo "Try checking http://t3serv001.mit.edu/~cmsprod/ConsistencyChecks/$SiteName/${SiteName}_skipCksm_removable.txt"
    exit
fi

options=("Does not delete files, but shows files to be deleted with 2 second pauses an entire directories"
         "Does not delete files, but shows files to be deleted without pauses"
         "Deletes files, but with 2 second pauses before clearing an entire directory to allow interruptions"
         "Deletes files, but without pauses" "Quit")
echo 'Please select an option:'
select opt in "${options[@]}"; do
    case $opt in 
        "${options[0]}")
            echo ./ClearSite.py --safe -T $SiteName
            ./ClearSite.py --safe -T $SiteName
            break;;
        "${options[1]}")
            echo ./ClearSite.py --fast --safe -T $SiteName
            ./ClearSite.py --fast --safe -T $SiteName
            break;;
        "${options[2]}")
            echo ./ClearSite.py -T $SiteName
            ./ClearSite.py -T $SiteName
            break;;
        "${options[3]}")
            echo ./ClearSite.py --fast -T $SiteName
            ./ClearSite.py --fast -T $SiteName
            break;;
        "${options[4]}")
            echo "Quitting without clearing..."
            break;;
        *)
            echo "Invalid Option..."
            echo "Quitting without clearing..."
            break;;
    esac
done
