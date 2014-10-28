#! /bin/bash

# Carefully clears a directory and removes empty directories left behind

dir=$1

returnTo=`pwd`

cd $dir                        # Go to specified directory
echo 'Removing directory:'
echo '**************************************************************************************'
echo `pwd`
echo '**************************************************************************************'
echo 'Hit Ctrl-C to cancel.'
read -t 5 -p 'Hit enter or wait 5 seconds to continue.'

for term in `ls -A`; do        # Get everything in that directory
    if [ -f $term ]; then      # Remove everything that is a file
        rm $term
    fi
done

while true; do                 # Continue this until directory is not empty
    if [ ! "$(ls -A)" ]; then  # If empty, cd out and remove directory
        dirToRemove=`pwd`
        cd ..
        rmdir $dirToRemove
    else                       # If not empty, stop the loop
        break
    fi
done

cd $returnTo                   # Return to original working directory for fun
