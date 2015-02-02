#! /bin/bash

SiteName=$1

wget -q -N http://t3serv001.mit.edu/~cmsprod/ConsistencyChecks/ClearSite.py                                # Download ClearSite.py, if it's new
wget -q -N http://t3serv001.mit.edu/~cmsprod/ConsistencyChecks/$SiteName/$SiteName\_skipCksm_results.txt   # Download results list, if it's new

options=("Does not delete files, but shows files to be deleted with 2 second pauses between directories"
         "Does not delete files, but shows files to be deleted without pauses"
         "Deletes files, but with 2 second pauses to allow interruptions"
         "Deletes files, but without pauses" "Quit")
echo 'Please select an option:'
select opt in "${options[@]}"; do
    case $opt in 
        "${options[0]}")
            echo python ClearSite.py --safe -T $SiteName
            break;;
        "${options[1]}")
            echo python ClearSite.py --fast --safe -T $SiteName
            break;;
        "${options[2]}")
            echo python ClearSite.py -T $SiteName
            break;;
        "${options[3]}")
            echo python ClearSite.py --fast -T $SiteName
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