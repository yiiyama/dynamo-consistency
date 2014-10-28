#! /bin/bash

# Carefully clears a directory and removes empty directories left behind

dir=$1

returnTo=`pwd`

cd $dir                        # Go to specified directory

for term in `ls -A`; do        # Get everything in that directory
    if [ -f $term ]; then      # Remove everything that is a file
        rm $term
    fi
done

empty=true
while [ $empty ]; do           # Continue this until directory is not empty
    if [ ! "$(ls -A)" ]; then  # If empty, cd out and remove directory
        dirToRemove=`pwd`
        cd ..
        rmdir $dirToRemove
    else                       # If not empty, stop the loop
        empty=false
    fi
done

cd $returnTo                   # Return to original working directory for fun
