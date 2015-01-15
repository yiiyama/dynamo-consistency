#! /bin/bash

# This is a nice little script that submits jobs for me and cleans up my space and website

# This is where I am keeping all of the output:
STOREDIR='/scratch/dabercro/ConsistencyCheck/crabRuns'
FAILDIR='/home/dabercro/FailedCheck'
SERVERDIR='/home/cmsprod/public_html'

# "T2_US_Nebraska"
declare -a SITES=("T2_US_Caltech" "T2_US_Florida" "T2_US_MIT" "T2_US_Purdue" "T2_US_UCSD" "T2_US_Vanderbilt" "T2_US_Wisconsin")

for SITE in "${SITES[@]}";do
    COUNTDIR=`ls -d $SITE-* | wc -l`
    if [ "$COUNTDIR" -eq "1" ]; then                             # Ideally, there will only be one directory per site
        DIRNAME=`ls -d $SITE-*`
        echo $DIRNAME
        DIRCHECK=`ls $DIRNAME/res | wc -l`                       # Check if any results have been posted
        if [ "$DIRCHECK" -eq "0" ]; then                         # If not, try to retrieve output
            ./check.sh 
        else
            ISTAR=`ls $DIRNAME/res/$SITE.tar*.gz | wc -l`        # Look for the tarred output
            if [ "$ISTAR" -eq "1" ]; then                        # If it's there, update the website
                tar -xzvf $DIRNAME/res/$SITE.tar*.gz
                cp $SITE*results.txt $SERVERDIR/$SITE/.
            elif [ "$ISTAR" -eq "0" ]; then                      # If there's no tar, check to see if the job finished
                if [ -f $DIRNAME/res/CMSSW_1.stdout ]; then      # If it did (with no tar) then there must have been an error
                    echo "Looks like the job failed... Check that out later."
                    cp -r $DIRNAME $STOREDIR/.
                    mv $DIRNAME $FAILDIR/.
                fi
            else
                echo "How did I get more tars??"
                echo "I'll just hide those and start over!"
                mv $DIRNAME $STOREDIR/.
            fi
        fi
    else
        if [ "$COUNTDIR" -gt "1" ]; then
            echo "There are too many directories from $SITE!!" 
            echo "I'll just hide those and start over!"
            mv $SITE-* $STOREDIR/.
        fi
        NOW=`date +"%Y-%m-%d-%T"`
        echo ./submit.sh $SITE $NOW
#        ./submit.sh $SITE $NOW
    fi
done
